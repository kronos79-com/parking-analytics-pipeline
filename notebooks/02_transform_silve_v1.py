# Databricks notebook source
# ─── NOTEBOOK: 02_transform_silver.py ────────────────────────────────────────
#
# PURPOSE
# -------
# Read raw Bronze Delta tables and produce clean, conformed Silver tables.
# Each provider gets its own Silver table first (provider-specific cleansing),
# then all three are written to a unified Silver table ready for Gold.
#
# WHAT THIS NOTEBOOK DOES
# -----------------------
# 1. CLEANSE   : Fix provider-specific data quality problems
#                - ParkTech  : null charge_amount, UNKNOWN payment_method,
#                              duplicate transaction_ids
#                - VendPark  : mixed-case payment_type, UK date format,
#                              exit before entry, null duration
#                - EasyEntry : £ symbol in charge, missing exit_time,
#                              negative duration
#
# 2. CONFORM   : Rename all columns to a single canonical schema so all
#                three providers look identical downstream
#
# 3. ENRICH    : Join to the site map to resolve provider-specific site
#                identifiers to a canonical_site_id (SITE-001 etc.)
#
# 4. UNIFY     : Union all three conformed datasets into one Silver table
#
# CANONICAL SCHEMA (output of this notebook)
# ------------------------------------------
#   transaction_id    : string  - unique transaction reference
#   canonical_site_id : string  - SITE-001 through SITE-010
#   site_name         : string  - human readable site name
#   city              : string  - city from site map
#   capacity          : integer - car park capacity from site map
#   entry_timestamp   : timestamp
#   exit_timestamp    : timestamp  (null if missing)
#   duration_minutes  : integer    (null if invalid)
#   vehicle_type      : string
#   payment_method    : string  - normalised to UPPER_SNAKE_CASE
#   card_scheme       : string
#   charge_amount     : double  - always numeric, never null (0.0 for free)
#   discount_code     : string  - null if no discount
#   product_id        : string  - null if no discount
#   psp               : string
#   acquirer          : string
#   psp_reference     : string
#   provider          : string  - PARKTECH, VENDPARK, EASYENTRY
#   _ingested_at      : timestamp - carried from Bronze
#   _source_file      : string    - carried from Bronze
#   _cleansed_at      : timestamp - added by this notebook
#
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Transformation
# MAGIC ## Parking Analytics Pipeline
# MAGIC Cleanses, conforms, and enriches Bronze data into a unified Silver table.

# COMMAND ----------

# ─── Imports ─────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, upper, trim, regexp_replace,
    to_timestamp, when, abs as spark_abs,
    current_timestamp, coalesce, count,
    row_number
)
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ─── Configuration ────────────────────────────────────────────────────────────

BRONZE_DATABASE = "parking_bronze"
SILVER_DATABASE = "parking_silver"

print(f"Reading from : {BRONZE_DATABASE}")
print(f"Writing to   : {SILVER_DATABASE}")

# COMMAND ----------

# ─── Create Silver database ───────────────────────────────────────────────────

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE}")
spark.sql(f"USE {SILVER_DATABASE}")
print(f"Using database: {SILVER_DATABASE}")

# COMMAND ----------

# ─── Load Bronze tables ───────────────────────────────────────────────────────
#
# Always read from Bronze, never from the source CSV directly.
# Bronze is your immutable record of what arrived. Silver reads from that.

bronze_parktech  = spark.table(f"{BRONZE_DATABASE}.bronze_parktech")
bronze_vendpark  = spark.table(f"{BRONZE_DATABASE}.bronze_vendpark")
bronze_easyentry = spark.table(f"{BRONZE_DATABASE}.bronze_easyentry")
bronze_site_map  = spark.table(f"{BRONZE_DATABASE}.bronze_site_map")

print("Bronze tables loaded:")
print(f"  ParkTech  : {bronze_parktech.count():,} rows")
print(f"  VendPark  : {bronze_vendpark.count():,} rows")
print(f"  EasyEntry : {bronze_easyentry.count():,} rows")
print(f"  Site map  : {bronze_site_map.count():,} rows")

# COMMAND ----------

# ─── Prepare site map for joins ───────────────────────────────────────────────
#
# Cast capacity to integer now that we trust the source is clean.
# We will join this three times (once per provider) using different key columns.

site_map = (
    bronze_site_map
    .select(
        col("canonical_site_id"),
        col("site_name").alias("canonical_site_name"),
        col("city"),
        col("capacity").cast(IntegerType()).alias("capacity"),
        col("parktech_id"),
        col("vendpark_slug"),
        col("easyentry_uuid"),
    )
)

print("Site map prepared:")
site_map.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider 1: ParkTech Cleansing
# MAGIC
# MAGIC **Problems to fix:**
# MAGIC - `charge_amount` : occasionally null - default to 0.0
# MAGIC - `payment_method`: occasionally "UNKNOWN" - set to null (unknown is not a method)
# MAGIC - `transaction_id` : occasional duplicates - keep first occurrence only
# MAGIC - `duration_minutes`: already integer-compatible strings, just cast
# MAGIC - Timestamps       : ISO 8601 format, straightforward to cast

# COMMAND ----------

def cleanse_parktech(df: DataFrame, site_map: DataFrame) -> DataFrame:
    """
    Cleanses ParkTech Bronze data and conforms to canonical Silver schema.

    Key decisions:
    - Duplicate transaction_ids: we keep the first occurrence ordered by
      entry_timestamp. In production this decision would be documented
      and signed off by the business. We never silently drop rows without
      a documented reason.
    - UNKNOWN payment_method: replaced with null rather than a default value.
      Null is honest. Inventing a value would corrupt downstream analysis.
    - Null charge_amount: defaulted to 0.0. A null charge on a real transaction
      is more likely a data feed issue than a genuinely free transaction.
      Free transactions have an explicit discount code. We flag these rows
      with a note in a real system; here we default and document.
    """

    # Step 1: Deduplicate on transaction_id, keep earliest entry
    window = Window.partitionBy("transaction_id").orderBy("entry_timestamp")
    df = (
        df
        .withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    # Step 2: Cast and cleanse
    df = (
        df
        .withColumn(
            "charge_amount",
            coalesce(
                col("charge_amount").cast(DoubleType()),
                lit(0.0)                                    # null charge -> 0.0
            )
        )
        .withColumn(
            "payment_method",
            when(col("payment_method") == "UNKNOWN", None)  # UNKNOWN -> null
            .otherwise(upper(trim(col("payment_method"))))
        )
        .withColumn(
            "duration_minutes",
            col("duration_minutes").cast(IntegerType())
        )
        .withColumn(
            "entry_timestamp",
            to_timestamp(col("entry_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        .withColumn(
            "exit_timestamp",
            to_timestamp(col("exit_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
    )

    # Step 3: Join to site map on ParkTech site_id
    df = df.join(
        site_map.select("canonical_site_id", "canonical_site_name", "city", "capacity", "parktech_id"),
        df["site_id"] == site_map["parktech_id"],
        how="left"
    )

    # Step 4: Conform to canonical schema
    return (
        df
        .select(
            col("transaction_id"),
            col("canonical_site_id"),
            col("canonical_site_name").alias("site_name"),
            col("city"),
            col("capacity"),
            col("entry_timestamp"),
            col("exit_timestamp"),
            col("duration_minutes"),
            col("vehicle_type"),
            col("payment_method"),
            col("card_scheme"),
            col("charge_amount"),
            col("discount_code"),
            col("product_id"),
            col("psp"),
            col("acquirer"),
            col("psp_reference"),
            lit("PARKTECH").alias("provider"),
            col("_ingested_at"),
            col("_source_file"),
            current_timestamp().alias("_cleansed_at"),
        )
    )


silver_parktech = cleanse_parktech(bronze_parktech, site_map)
print(f"ParkTech Silver : {silver_parktech.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider 2: VendPark Cleansing
# MAGIC
# MAGIC **Problems to fix:**
# MAGIC - `payment_type`  : mixed case (DEBIT_CARD, debit_card, Debit_Card) - normalise to UPPER_SNAKE_CASE
# MAGIC - `arrival`       : UK date format dd/MM/yyyy HH:mm - parse correctly
# MAGIC - `departure`     : same as arrival, plus some exit before entry - set those to null
# MAGIC - `stay_minutes`  : occasionally null - recalculate from timestamps where possible
# MAGIC - `amount_charged`: already numeric string, just cast

# COMMAND ----------

def cleanse_vendpark(df: DataFrame, site_map: DataFrame) -> DataFrame:
    """
    Cleanses VendPark Bronze data and conforms to canonical Silver schema.

    Key decisions:
    - Mixed case payment_type: upper() + trim() + replace spaces with underscore
      handles all variants cleanly.
    - Exit before entry: these rows have an impossible duration. We set
      exit_timestamp to null and duration to null rather than dropping the row.
      The transaction still happened, the timestamps are just unreliable.
    - Null duration: where we have valid entry and exit timestamps we
      recalculate duration. Where we cannot, we leave as null.
    - UK date format: to_timestamp with explicit format pattern. If the
      pattern does not match, Spark returns null rather than throwing,
      which is safe behaviour.
    """

    # Step 1: Normalise payment_type to UPPER_SNAKE_CASE
    # Handles: "debit_card", "Debit_Card", "DEBIT CARD", "Debit Card" etc.
    df = df.withColumn(
        "payment_type_clean",
        upper(
            regexp_replace(
                trim(col("payment_type")),
                r"[\s]+", "_"               # spaces to underscores
            )
        )
    )

    # Step 2: Parse UK format timestamps
    df = (
        df
        .withColumn(
            "entry_timestamp",
            to_timestamp(col("arrival"), "dd/MM/yyyy HH:mm")
        )
        .withColumn(
            "exit_timestamp_raw",
            to_timestamp(col("departure"), "dd/MM/yyyy HH:mm")
        )
    )

    # Step 3: Fix exit before entry - set exit and duration to null
    df = (
        df
        .withColumn(
            "exit_timestamp",
            when(
                col("exit_timestamp_raw") < col("entry_timestamp"),
                None                        # impossible - nullify
            ).otherwise(col("exit_timestamp_raw"))
        )
    )

    # Step 4: Recalculate duration where possible
    # If stay_minutes is null but we have valid timestamps, derive it.
    # Cast to integer first, negative or blank becomes null.
    df = (
        df
        .withColumn(
            "duration_raw",
            col("stay_minutes").cast(IntegerType())
        )
        .withColumn(
            "duration_minutes",
            when(
                col("duration_raw").isNotNull() & (col("duration_raw") > 0),
                col("duration_raw")
            ).when(
                col("exit_timestamp").isNotNull() & col("entry_timestamp").isNotNull(),
                ((col("exit_timestamp").cast("long") - col("entry_timestamp").cast("long")) / 60)
                .cast(IntegerType())        # recalculate from timestamps
            ).otherwise(None)
        )
    )

    # Step 5: Cast charge
    df = df.withColumn(
        "charge_amount",
        col("amount_charged").cast(DoubleType())
    )

    # Step 6: Join to site map on VendPark slug
    df = df.join(
        site_map.select("canonical_site_id", "canonical_site_name", "city", "capacity", "vendpark_slug"),
        df["car_park_slug"] == site_map["vendpark_slug"],
        how="left"
    )

    # Step 7: Conform to canonical schema
    return (
        df
        .select(
            col("txn_ref").alias("transaction_id"),
            col("canonical_site_id"),
            col("canonical_site_name").alias("site_name"),
            col("city"),
            col("capacity"),
            col("entry_timestamp"),
            col("exit_timestamp"),
            col("duration_minutes"),
            col("vehicle").alias("vehicle_type"),
            col("payment_type_clean").alias("payment_method"),
            col("card_scheme"),
            col("charge_amount"),
            col("discount_code"),
            col("product_id"),
            col("psp"),
            col("acquirer"),
            col("psp_reference"),
            lit("VENDPARK").alias("provider"),
            col("_ingested_at"),
            col("_source_file"),
            current_timestamp().alias("_cleansed_at"),
        )
    )


silver_vendpark = cleanse_vendpark(bronze_vendpark, site_map)
print(f"VendPark Silver : {silver_vendpark.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider 3: EasyEntry Cleansing
# MAGIC
# MAGIC **Problems to fix:**
# MAGIC - `charge`        : string with £ symbol (e.g. "£5.00") - strip and cast
# MAGIC - `exit_time`     : occasionally missing - set to null
# MAGIC - `duration_mins` : occasionally negative - take absolute value as best estimate
# MAGIC                     recalculate from timestamps where exit is missing

# COMMAND ----------

def cleanse_easyentry(df: DataFrame, site_map: DataFrame) -> DataFrame:
    """
    Cleanses EasyEntry Bronze data and conforms to canonical Silver schema.

    Key decisions:
    - £ symbol removal: regexp_replace strips any non-numeric character
      except the decimal point. Safer than just stripping £ specifically
      in case the source ever sends $ or EUR symbol instead.
    - Negative duration: we take the absolute value as a best estimate.
      A duration of -45 almost certainly means 45 minutes with a sign
      error in the source system. Documenting this assumption matters.
    - Missing exit_time: we leave exit_timestamp as null. We do NOT
      invent an exit time. Duration is also null in this case.
    """

    # Step 1: Strip currency symbol and cast charge to double
    # regexp_replace removes anything that is not a digit or decimal point
    df = df.withColumn(
        "charge_amount",
        regexp_replace(col("charge"), r"[^\d.]", "").cast(DoubleType())
    )

    # Step 2: Parse ISO 8601 timestamps (same format as ParkTech)
    df = (
        df
        .withColumn(
            "entry_timestamp",
            to_timestamp(col("entry_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        .withColumn(
            "exit_timestamp",
            to_timestamp(col("exit_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'")  # null if empty
        )
    )

    # Step 3: Fix duration
    # Cast to integer (empty string and text become null)
    # Negative values: take absolute value
    # Missing entirely: recalculate from timestamps if available
    df = (
        df
        .withColumn(
            "duration_raw",
            col("duration_mins").cast(IntegerType())
        )
        .withColumn(
            "duration_minutes",
            when(
                col("duration_raw").isNotNull() & (col("duration_raw") < 0),
                spark_abs(col("duration_raw"))      # negative -> positive
            ).when(
                col("duration_raw").isNotNull() & (col("duration_raw") > 0),
                col("duration_raw")
            ).when(
                col("exit_timestamp").isNotNull() & col("entry_timestamp").isNotNull(),
                ((col("exit_timestamp").cast("long") - col("entry_timestamp").cast("long")) / 60)
                .cast(IntegerType())
            ).otherwise(None)
        )
    )

    # Step 4: Normalise payment_method to UPPER_SNAKE_CASE for consistency
    df = df.withColumn(
        "payment_method",
        upper(trim(col("payment_method")))
    )

    # Step 5: Join to site map on EasyEntry UUID
    df = df.join(
        site_map.select("canonical_site_id", "canonical_site_name", "city", "capacity", "easyentry_uuid"),
        df["site_uuid"] == site_map["easyentry_uuid"],
        how="left"
    )

    # Step 6: Conform to canonical schema
    return (
        df
        .select(
            col("id").alias("transaction_id"),
            col("canonical_site_id"),
            col("canonical_site_name").alias("site_name"),
            col("city"),
            col("capacity"),
            col("entry_timestamp"),
            col("exit_timestamp"),
            col("duration_minutes"),
            col("vehicle_class").alias("vehicle_type"),
            col("payment_method"),
            col("card_scheme"),
            col("charge_amount"),
            col("discount_code"),
            col("product_id"),
            col("psp"),
            col("acquirer"),
            col("psp_reference"),
            lit("EASYENTRY").alias("provider"),
            col("_ingested_at"),
            col("_source_file"),
            current_timestamp().alias("_cleansed_at"),
        )
    )


silver_easyentry = cleanse_easyentry(bronze_easyentry, site_map)
print(f"EasyEntry Silver : {silver_easyentry.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unify: Union all three providers into one Silver table

# COMMAND ----------

# ─── Union all three providers ────────────────────────────────────────────────
#
# All three DataFrames now share identical schemas.
# unionByName is safer than union() because it matches columns by name
# rather than position, so a column order difference cannot silently
# corrupt data.

silver_unified = (
    silver_parktech
    .unionByName(silver_vendpark)
    .unionByName(silver_easyentry)
)

total = silver_unified.count()
print(f"Unified Silver total : {total:,} rows")

# COMMAND ----------

# ─── Write Silver tables ──────────────────────────────────────────────────────
#
# We write two tables:
# 1. silver_transactions_unified : the full merged dataset for Gold to consume
# 2. Individual provider tables  : useful for provider-specific analysis
#    and debugging. If Gold produces a suspect number you can quickly
#    isolate which provider contributed it.

print("\nWriting Silver tables...")

# Individual provider tables
for name, df in [
    ("silver_parktech",  silver_parktech),
    ("silver_vendpark",  silver_vendpark),
    ("silver_easyentry", silver_easyentry),
]:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_DATABASE}.{name}")
    )
    print(f"  Written : {SILVER_DATABASE}.{name}")

# Unified table
(
    silver_unified.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_DATABASE}.silver_transactions_unified")
)
print(f"  Written : {SILVER_DATABASE}.silver_transactions_unified")

# COMMAND ----------

# ─── Verification ─────────────────────────────────────────────────────────────

print("\nVerification - Silver table row counts:")
print("-" * 55)

tables = [
    "silver_parktech",
    "silver_vendpark",
    "silver_easyentry",
    "silver_transactions_unified",
]

for t in tables:
    count = spark.table(f"{SILVER_DATABASE}.{t}").count()
    print(f"  {t:<35} {count:>8,} rows")

# COMMAND ----------

# ─── Data quality checks ──────────────────────────────────────────────────────
#
# Always validate the output of a cleansing step.
# These checks confirm the specific problems from Bronze have been resolved.

print("\nData quality checks on unified Silver:")
print("-" * 55)

unified = spark.table(f"{SILVER_DATABASE}.silver_transactions_unified")
total   = unified.count()

# 1. No £ symbols remaining in charge_amount (it is now a double)
null_charges = unified.filter(col("charge_amount").isNull()).count()
print(f"  Null charge_amount      : {null_charges:,}  (expect ~0)")

# 2. No UNKNOWN payment methods
unknown_methods = unified.filter(col("payment_method") == "UNKNOWN").count()
print(f"  UNKNOWN payment_method  : {unknown_methods:,}  (expect 0)")

# 3. No mixed case payment methods - all should be UPPER_SNAKE_CASE
mixed_case = unified.filter(
    col("payment_method") != upper(col("payment_method"))
).count()
print(f"  Mixed-case payment_method : {mixed_case:,}  (expect 0)")

# 4. No negative durations
negative_dur = unified.filter(
    col("duration_minutes").isNotNull() & (col("duration_minutes") < 0)
).count()
print(f"  Negative duration_minutes : {negative_dur:,}  (expect 0)")

# 5. All rows have a canonical_site_id
missing_site = unified.filter(col("canonical_site_id").isNull()).count()
print(f"  Missing canonical_site_id : {missing_site:,}  (expect 0)")

# 6. Provider breakdown
print("\nRow count by provider:")
(
    unified
    .groupBy("provider")
    .count()
    .orderBy("provider")
    .show()
)

# 7. Acquirer breakdown - confirms AMEX/Worldpay routing is intact
print("Acquirer breakdown:")
(
    unified
    .groupBy("acquirer")
    .count()
    .orderBy("acquirer")
    .show()
)

# COMMAND ----------

# ─── Side by side comparison: Bronze vs Silver ────────────────────────────────
#
# This is the money shot. Show the same data before and after cleansing.
# Useful for interviews and for the README.

print("EasyEntry - Bronze (raw £ symbol in charge):")
spark.table(f"{BRONZE_DATABASE}.bronze_easyentry") \
    .select("id", "charge", "duration_mins", "exit_time") \
    .show(5, truncate=False)

print("EasyEntry - Silver (charge as double, duration fixed):")
spark.table(f"{SILVER_DATABASE}.silver_easyentry") \
    .select("transaction_id", "charge_amount", "duration_minutes", "exit_timestamp") \
    .show(5, truncate=False)

print("VendPark - Bronze (mixed case payment_type):")
spark.table(f"{BRONZE_DATABASE}.bronze_vendpark") \
    .select("txn_ref", "payment_type", "arrival", "departure") \
    .show(5, truncate=False)

print("VendPark - Silver (normalised payment_method, parsed timestamps):")
spark.table(f"{SILVER_DATABASE}.silver_vendpark") \
    .select("transaction_id", "payment_method", "entry_timestamp", "exit_timestamp") \
    .show(5, truncate=False)