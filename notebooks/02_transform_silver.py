# Databricks notebook source
# ─── NOTEBOOK: 02_transform_silver.py ────────────────────────────────────────
#
# PURPOSE
# -------
# Read raw Bronze Delta tables and produce clean, conformed Silver tables.
#
# WHAT THIS NOTEBOOK DOES
# -----------------------
# 1. CLEANSE   : Fix provider-specific data quality problems
#                ParkTech  : null gross_charge, UNKNOWN payment_method,
#                            duplicate transaction_ids
#                VendPark  : mixed-case payment_type, UK date format,
#                            exit before entry, null duration
#                EasyEntry : £ symbol on all price fields, vat_pct as integer
#                            string, missing exit/entry on barrierless sites,
#                            negative duration on barrier sites
#
# 2. CONFORM   : Rename all columns to canonical schema. Normalise VendPark
#                NET financials to GROSS basis. Derive discount_amount for
#                VendPark and EasyEntry. Compute canonical net financial fields
#                for all providers.
#
# 3. ENRICH    : Join to site map for canonical_site_id, site_type,
#                payment_type, open/close hours, ticket_prefix.
#
# 4. UNIFY     : Union all three providers into one Silver table.
#
# FINANCIAL NORMALISATION
# -----------------------
# All providers are normalised to the same financial schema regardless of
# whether the source reported gross or net:
#
#   gross_charge         : full undiscounted tariff, VAT inclusive
#   discount_amount      : value of discount, VAT inclusive
#   discounted_gross     : gross_charge minus discount_amount
#   vat_rate             : decimal (0.20)
#   vat_amount           : VAT element of discounted_gross
#   net_charge           : gross_charge / 1.20  (pre-discount, ex-VAT)
#   net_discount_amount  : discount_amount / 1.20
#   net_amount_paid      : discounted_gross / 1.20
#   amount_paid          : discounted_gross (VAT inclusive)
#
# VendPark reports NET. Silver converts to gross before populating this schema:
#   gross = net * 1.20
#
# DURATION FIELDS
# ---------------
#   duration_minutes      : actual measured duration (barrier sites only)
#   paid_duration_minutes : maximum of gross tariff band paid (barrierless only)
#                           reflects entitlement granted before any discount
#
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Transformation
# MAGIC ## Parking Analytics Pipeline
# MAGIC Cleanses, conforms, normalises financials, and enriches Bronze data
# MAGIC into a unified Silver table.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, upper, trim, regexp_replace,
    to_timestamp, when, abs as spark_abs,
    current_timestamp, coalesce, round as spark_round,
    row_number, sum as spark_sum
)
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

BRONZE_DATABASE = "parking_bronze"
SILVER_DATABASE = "parking_silver"
VAT_RATE        = 0.20
VAT_DIVISOR     = 1 + VAT_RATE

print(f"Reading from : {BRONZE_DATABASE}")
print(f"Writing to   : {SILVER_DATABASE}")
print(f"VAT rate     : {VAT_RATE}")

# COMMAND ----------

# ─── Tariff band maximums for paid_duration_minutes ───────────────────────────
#
# Barrierless sites: paid_duration_minutes = maximum minutes of the gross
# tariff band the customer paid for. Reflects entitlement before any discount.

TARIFF_BANDS = {
    "3a1b2c3d-4e5f-6789-abcd-ef0123456702": [  # The Lanes Short Stay
        (60, 1.50), (120, 2.50), (240, 4.00),
        (360, 5.50), (720, 7.50), (1440, 11.00)
    ],
    "3a1b2c3d-4e5f-6789-abcd-ef0123456704": [  # North Street Surface
        (60, 1.00), (120, 2.00), (240, 3.00),
        (360, 4.00), (720, 6.00), (1440, 8.00)
    ],
}

def build_paid_duration_expr(site_uuid_col, gross_charge_col):
    expr = lit(None).cast(IntegerType())
    for site_id, bands in TARIFF_BANDS.items():
        for (max_min, price) in bands:
            expr = when(
                (col(site_uuid_col) == site_id) &
                (col(gross_charge_col) == price),
                max_min
            ).otherwise(expr)
    return expr

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE}")
spark.sql(f"USE {SILVER_DATABASE}")
print(f"Using database: {SILVER_DATABASE}")

# COMMAND ----------

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

site_map = (
    bronze_site_map
    .select(
        col("canonical_site_id"),
        col("site_name").alias("canonical_site_name"),
        col("city"),
        col("capacity").cast(IntegerType()).alias("capacity"),
        col("site_type"),
        col("payment_type").alias("site_payment_type"),
        col("open_hour").cast(IntegerType()).alias("open_hour"),
        col("close_hour").cast(IntegerType()).alias("close_hour"),
        col("ticket_prefix"),
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
# MAGIC Source reports GROSS (VAT inclusive). `discount_amount` is explicit.
# MAGIC Net fields derived by dividing gross by 1.20.

# COMMAND ----------

def cleanse_parktech(df: DataFrame, site_map: DataFrame) -> DataFrame:

    # Deduplicate on transaction_id - keep earliest entry
    window = Window.partitionBy("transaction_id").orderBy("entry_timestamp")
    df = (
        df
        .withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    df = (
        df
        .withColumn("entry_timestamp",  to_timestamp(col("entry_timestamp"),  "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("exit_timestamp",   to_timestamp(col("exit_timestamp"),   "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("transaction_time", to_timestamp(col("transaction_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))
        .withColumn(
            "payment_method",
            when(col("payment_method") == "UNKNOWN", None)
            .otherwise(upper(trim(col("payment_method"))))
        )
    )

    # Financial - GROSS source
    df = (
        df
        .withColumn("gross_charge_raw",
            col("gross_charge").cast(DoubleType()))
        .withColumn("gross_charge",
            col("gross_charge_raw"))
        .withColumn("discount_amount",
            when(col("gross_charge").isNull(), None)
            .otherwise(col("discount_amount").cast(DoubleType())))
        .withColumn("discounted_gross",
            when(col("gross_charge").isNull(), None)
            .otherwise(col("discounted_charge").cast(DoubleType())))
        .withColumn("vat_rate",
            when(col("gross_charge").isNull(), None)
            .otherwise(col("vat_rate").cast(DoubleType())))
        .withColumn("vat_amount",
            when(col("gross_charge").isNull(), None)
            .otherwise(col("vat_amount").cast(DoubleType())))
        .withColumn("amount_paid",
            when(col("gross_charge").isNull(), None)
            .otherwise(col("amount_paid").cast(DoubleType())))
        .withColumn("net_charge",
            when(col("gross_charge").isNull(), None)
            .otherwise(spark_round(col("gross_charge") / VAT_DIVISOR, 2)))
        .withColumn("net_discount_amount",
            when(col("gross_charge").isNull(), None)
            .otherwise(spark_round(col("discount_amount") / VAT_DIVISOR, 2)))
        .withColumn("net_amount_paid",
            when(col("gross_charge").isNull(), None)
            .otherwise(spark_round(col("amount_paid") / VAT_DIVISOR, 2)))
    )

    df = df.join(
        site_map.select("canonical_site_id","canonical_site_name","city","capacity",
                        "site_type","site_payment_type","parktech_id"),
        df["site_id"] == site_map["parktech_id"],
        how="left"
    )

    return df.select(
        col("transaction_id"),
        col("canonical_site_id"),
        col("canonical_site_name").alias("site_name"),
        col("city"),
        col("capacity"),
        col("site_type"),
        col("site_payment_type").alias("payment_type"),
        col("vrm"),
        lit(None).cast("string").alias("ticket_number"),
        col("entry_timestamp"),
        col("exit_timestamp"),
        col("transaction_time"),
        col("duration_minutes"),
        lit(None).cast(IntegerType()).alias("paid_duration_minutes"),
        col("payment_method"),
        col("card_scheme"),
        col("gross_charge"),
        col("discount_code"),
        col("product_id"),
        col("discount_amount"),
        col("discounted_gross"),
        col("vat_rate"),
        col("vat_amount"),
        col("net_charge"),
        col("net_discount_amount"),
        col("net_amount_paid"),
        col("amount_paid"),
        col("psp"),
        col("acquirer"),
        col("psp_reference"),
        lit("PARKTECH").alias("provider"),
        col("_ingested_at"),
        col("_source_file"),
        current_timestamp().alias("_cleansed_at"),
    )

silver_parktech = cleanse_parktech(bronze_parktech, site_map)
print(f"ParkTech Silver : {silver_parktech.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider 2: VendPark Cleansing
# MAGIC
# MAGIC Source reports NET (VAT exclusive). Convert to GROSS: gross = net * 1.20.
# MAGIC discount_amount derived from net_charge minus discounted_net, then grossed up.

# COMMAND ----------

def cleanse_vendpark(df: DataFrame, site_map: DataFrame) -> DataFrame:

    df = df.withColumn(
        "payment_type_clean",
        upper(regexp_replace(trim(col("payment_type")), r"[\s]+", "_"))
    )

    df = (
        df
        .withColumn("entry_timestamp",    to_timestamp(col("arrival"),          "dd/MM/yyyy HH:mm"))
        .withColumn("exit_timestamp_raw", to_timestamp(col("departure"),        "dd/MM/yyyy HH:mm"))
        .withColumn("transaction_time",   to_timestamp(col("transaction_time"), "dd/MM/yyyy HH:mm"))
    )

    df = df.withColumn(
        "exit_timestamp",
        when(col("exit_timestamp_raw") < col("entry_timestamp"), None)
        .otherwise(col("exit_timestamp_raw"))
    )

    df = (
        df
        .withColumn("duration_raw", col("stay_minutes").cast(IntegerType()))
        .withColumn(
            "duration_minutes",
            when(col("duration_raw").isNotNull() & (col("duration_raw") > 0), col("duration_raw"))
            .when(
                col("exit_timestamp").isNotNull() & col("entry_timestamp").isNotNull(),
                ((col("exit_timestamp").cast("long") - col("entry_timestamp").cast("long")) / 60)
                .cast(IntegerType())
            ).otherwise(None)
        )
    )

    # Rename source columns that conflict with canonical output names
    df = df \
        .withColumnRenamed("net_charge",     "src_net") \
        .withColumnRenamed("discounted_net", "src_disc_net") \
        .withColumnRenamed("vat_rate",       "src_vat_rate") \
        .withColumnRenamed("vat_amount",     "src_vat_amount") \
        .withColumnRenamed("amount_paid",    "src_amount_paid")

    # Cast source values
    df = df.withColumn("src_net",      col("src_net").cast(DoubleType()))
    df = df.withColumn("src_disc_net", col("src_disc_net").cast(DoubleType()))

    # Step 1: Gross from net
    df = df.withColumn("gross_charge",
        spark_round(col("src_net") * VAT_DIVISOR, 2))

    # Step 2: Discount amount and discounted gross on gross basis
    df = df.withColumn("discount_amount",
        spark_round(
            col("gross_charge") * (lit(1.0) - col("src_disc_net") / col("src_net")),
            2
        ))
    df = df.withColumn("discounted_gross",
        spark_round(col("gross_charge") - col("discount_amount"), 2))

    # Step 3: VAT extracted first, net derived by subtraction
    df = df.withColumn("vat_rate",    lit(VAT_RATE))
    df = df.withColumn("vat_amount",
        spark_round(col("discounted_gross") - col("discounted_gross") / VAT_DIVISOR, 2))
    df = df.withColumn("net_amount_paid",
        col("discounted_gross") - col("vat_amount"))
    df = df.withColumn("vat_on_gross",
        spark_round(col("gross_charge") - col("gross_charge") / VAT_DIVISOR, 2))
    df = df.withColumn("net_charge",
        col("gross_charge") - col("vat_on_gross"))
    df = df.withColumn("net_discount_amount",
        spark_round(col("discount_amount") - col("discount_amount") / VAT_DIVISOR, 2))
    df = df.withColumn("amount_paid", col("discounted_gross"))

    # Join to site map
    df = df.join(
        site_map.select("canonical_site_id","canonical_site_name","city","capacity",
                        "site_type","site_payment_type","vendpark_slug"),
        df["car_park_slug"] == site_map["vendpark_slug"],
        how="left"
    )

    return df.select(
        col("txn_ref").alias("transaction_id"),
        col("canonical_site_id"),
        col("canonical_site_name").alias("site_name"),
        col("city"),
        col("capacity"),
        col("site_type"),
        col("site_payment_type").alias("payment_type"),
        col("vrm"),
        lit(None).cast("string").alias("ticket_number"),
        col("entry_timestamp"),
        col("exit_timestamp"),
        col("transaction_time"),
        col("duration_minutes"),
        lit(None).cast(IntegerType()).alias("paid_duration_minutes"),
        col("payment_type_clean").alias("payment_method"),
        col("card_scheme"),
        col("gross_charge"),
        col("discount_code"),
        col("product_id"),
        col("discount_amount"),
        col("discounted_gross"),
        col("vat_rate"),
        col("vat_amount"),
        col("net_charge"),
        col("net_discount_amount"),
        col("net_amount_paid"),
        col("amount_paid"),
        col("psp"),
        col("acquirer"),
        col("psp_reference"),
        lit("VENDPARK").alias("provider"),
        col("_ingested_at"),
        col("_source_file"),
        current_timestamp().alias("_cleansed_at"),
    )

silver_vendpark = cleanse_vendpark(bronze_vendpark, site_map)
print(f"VendPark Silver : {silver_vendpark.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider 3: EasyEntry Cleansing
# MAGIC
# MAGIC Two sub-types:
# MAGIC - barrier_ticket: has entry/exit/duration/ticket_number, reports GROSS with £ symbol
# MAGIC - barrierless: no entry/exit/duration, paid_duration_minutes from gross tariff band

# COMMAND ----------

def debug_vendpark_cols(df, site_map):
    df = df.withColumn(
        "payment_type_clean",
        upper(regexp_replace(trim(col("payment_type")), r"[\s]+", "_"))
    )
    df = (
        df
        .withColumn("entry_timestamp",    to_timestamp(col("arrival"),    "dd/MM/yyyy HH:mm"))
        .withColumn("exit_timestamp_raw", to_timestamp(col("departure"),  "dd/MM/yyyy HH:mm"))
        .withColumn("transaction_time",   to_timestamp(col("transaction_time"), "dd/MM/yyyy HH:mm"))
    )
    df = df.withColumn(
        "exit_timestamp",
        when(col("exit_timestamp_raw") < col("entry_timestamp"), None)
        .otherwise(col("exit_timestamp_raw"))
    )
    df = (
        df
        .withColumn("duration_raw", col("stay_minutes").cast(IntegerType()))
        .withColumn(
            "duration_minutes",
            when(col("duration_raw").isNotNull() & (col("duration_raw") > 0), col("duration_raw"))
            .when(
                col("exit_timestamp").isNotNull() & col("entry_timestamp").isNotNull(),
                ((col("exit_timestamp").cast("long") - col("entry_timestamp").cast("long")) / 60)
                .cast(IntegerType())
            ).otherwise(None)
        )
    )
    
    print("Columns available before financial block:")
    print(df.columns)

debug_vendpark_cols(bronze_vendpark, site_map)

# COMMAND ----------

def cleanse_easyentry(df: DataFrame, site_map: DataFrame) -> DataFrame:

    # Strip £ from all price fields
    for src, tgt in [
        ("full_price",       "gross_charge_src"),
        ("discounted_price", "discounted_gross_src"),
        ("tax_amount",       "vat_amount_src"),
        ("charged",          "amount_paid_src"),
    ]:
        df = df.withColumn(tgt,
            regexp_replace(col(src), r"[^\d.]", "").cast(DoubleType()))

    # vat_pct is integer string "20" - convert to decimal
    df = df.withColumn("vat_rate", col("vat_pct").cast(DoubleType()) / 100)

    # Derive discount_amount from gross difference
    df = df.withColumn(
        "discount_amount",
        spark_round(col("gross_charge_src") - col("discounted_gross_src"), 2)
    )

    # Parse timestamps (empty strings become null)
    df = (
        df
        .withColumn("entry_timestamp",  to_timestamp(col("entry_time"),       "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("exit_timestamp",   to_timestamp(col("exit_time"),        "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("transaction_time", to_timestamp(col("transaction_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    )

    # Duration for barrier sites
    df = (
        df
        .withColumn("duration_raw", col("duration_mins").cast(IntegerType()))
        .withColumn(
            "duration_minutes",
            when(col("duration_raw").isNotNull() & (col("duration_raw") < 0),
                spark_abs(col("duration_raw")))
            .when(col("duration_raw").isNotNull() & (col("duration_raw") > 0),
                col("duration_raw"))
            .when(
                col("exit_timestamp").isNotNull() & col("entry_timestamp").isNotNull(),
                ((col("exit_timestamp").cast("long") - col("entry_timestamp").cast("long")) / 60)
                .cast(IntegerType()))
            .otherwise(None)
        )
    )

    # Join to site map
    df = df.join(
        site_map.select("canonical_site_id","canonical_site_name","city","capacity",
                        "site_type","site_payment_type","easyentry_uuid"),
        df["site_uuid"] == site_map["easyentry_uuid"],
        how="left"
    )

    # paid_duration_minutes for barrierless - uses gross tariff band (pre-discount)
    df = df.withColumn(
        "paid_duration_minutes",
        when(
            col("site_type") == "barrierless",
            build_paid_duration_expr("site_uuid", "gross_charge_src")
        ).otherwise(None)
    )

    # Net financial fields
    df = (
        df
        .withColumn("net_charge",          spark_round(col("gross_charge_src") / VAT_DIVISOR, 2))
        .withColumn("net_discount_amount", spark_round(col("discount_amount")  / VAT_DIVISOR, 2))
        .withColumn("net_amount_paid",     spark_round(col("discounted_gross_src") / VAT_DIVISOR, 2))
    )

    df = df.withColumn("payment_method", upper(trim(col("payment_method"))))

    return df.select(
        col("id").alias("transaction_id"),
        col("canonical_site_id"),
        col("canonical_site_name").alias("site_name"),
        col("city"),
        col("capacity"),
        col("site_type"),
        col("site_payment_type").alias("payment_type"),
        lit(None).cast("string").alias("vrm"),
        col("ticket_number"),
        col("entry_timestamp"),
        col("exit_timestamp"),
        col("transaction_time"),
        col("duration_minutes"),
        col("paid_duration_minutes"),
        col("payment_method"),
        col("card_scheme"),
        col("gross_charge_src").alias("gross_charge"),
        col("discount_code"),
        col("product_id"),
        col("discount_amount"),
        col("discounted_gross_src").alias("discounted_gross"),
        col("vat_rate"),
        col("vat_amount_src").alias("vat_amount"),
        col("net_charge"),
        col("net_discount_amount"),
        col("net_amount_paid"),
        col("amount_paid_src").alias("amount_paid"),
        col("psp"),
        col("acquirer"),
        col("psp_reference"),
        lit("EASYENTRY").alias("provider"),
        col("_ingested_at"),
        col("_source_file"),
        current_timestamp().alias("_cleansed_at"),
    )

silver_easyentry = cleanse_easyentry(bronze_easyentry, site_map)
print(f"EasyEntry Silver : {silver_easyentry.count():,} rows")

# COMMAND ----------

silver_unified = (
    silver_parktech
    .unionByName(silver_vendpark)
    .unionByName(silver_easyentry)
)
print(f"Unified Silver total : {silver_unified.count():,} rows")

# COMMAND ----------

print("\nWriting Silver tables...")

for name, df in [
    ("silver_parktech",             silver_parktech),
    ("silver_vendpark",             silver_vendpark),
    ("silver_easyentry",            silver_easyentry),
    ("silver_transactions_unified", silver_unified),
]:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_DATABASE}.{name}")
    )
    print(f"  Written : {SILVER_DATABASE}.{name}")

# COMMAND ----------

print("\nVerification - Silver table row counts:")
print("-" * 55)
for t in ["silver_parktech","silver_vendpark","silver_easyentry","silver_transactions_unified"]:
    c = spark.table(f"{SILVER_DATABASE}.{t}").count()
    print(f"  {t:<40} {c:>8,} rows")

# COMMAND ----------

from pyspark.sql.functions import abs as spark_abs

unified.withColumn(
    "diff",
    col("discount_amount") + col("discounted_gross") - col("gross_charge")
).filter(
    col("diff").isNotNull()
).selectExpr(
    "min(diff) as min_diff",
    "max(diff) as max_diff",
    "avg(diff) as avg_diff",
    "count(*) as total_rows",
    "sum(case when abs(diff) > 0.01 then 1 else 0 end) as over_1p",
    "sum(case when abs(diff) > 0.001 then 1 else 0 end) as over_0p1",
    "sum(case when abs(diff) != 0 then 1 else 0 end) as any_diff",
).show()

# COMMAND ----------

unified.withColumn(
    "diff",
    col("discount_amount") + col("discounted_gross") - col("gross_charge")
).filter(col("diff") > 1.0) \
.select(
    "provider", "canonical_site_id", "discount_code",
    "gross_charge", "discount_amount", "discounted_gross",
    "diff"
).show(20, truncate=False)

# COMMAND ----------

for table in ["silver_vendpark", "silver_easyentry", "silver_parktech"]:
    print(f"\n{table}:")
    spark.table(f"parking_silver.{table}").withColumn(
        "diff",
        col("discount_amount") + col("discounted_gross") - col("gross_charge")
    ).filter(
        spark_abs(col("diff")) > 0.01
    ).select(
        "transaction_id",
        "discount_code",
        "gross_charge",
        "discount_amount",
        "discounted_gross",
        "diff"
    ).show(20, truncate=False)

# COMMAND ----------

print("\nData quality checks:")
print("-" * 55)
unified = spark.table(f"{SILVER_DATABASE}.silver_transactions_unified")

checks = [
    ("Null gross_charge (dirty source rows)", unified.filter(col("gross_charge").isNull()).count(), "expect ~537"),
    ("UNKNOWN payment_method",             unified.filter(col("payment_method") == "UNKNOWN").count(),    "expect 0"),
    ("Mixed-case payment_method",          unified.filter(col("payment_method") != upper(col("payment_method"))).count(), "expect 0"),
    ("Negative duration_minutes",          unified.filter(col("duration_minutes").isNotNull() & (col("duration_minutes") < 0)).count(), "expect 0"),
    ("Missing canonical_site_id",          unified.filter(col("canonical_site_id").isNull()).count(),     "expect 0"),
    ("Barrierless with duration",          unified.filter((col("site_type") == "barrierless") & col("duration_minutes").isNotNull()).count(), "expect 0"),
    ("Barrier with paid_duration",         unified.filter((col("site_type") != "barrierless") & col("paid_duration_minutes").isNotNull()).count(), "expect 0"),
    ("net*1.2 != amount_paid",             unified.filter(spark_round(col("net_amount_paid") * 1.20, 1) != spark_round(col("amount_paid"), 1)).count(), "expect 0"),
    ("discount+discounted != gross (>1p tolerance)", unified.filter(spark_abs(col("discount_amount") + col("discounted_gross") - col("gross_charge")) > 0.01).count(), "expect 0"),
]

for label, result, note in checks:
    print(f"  {label:<45} {result:>6,}  ({note})")

# COMMAND ----------

print("\nFinancial summary by provider:")
(
    unified
    .groupBy("provider")
    .agg(
        spark_round(spark_sum("gross_charge"),    2).alias("total_gross"),
        spark_round(spark_sum("discount_amount"), 2).alias("total_discounts"),
        spark_round(spark_sum("vat_amount"),      2).alias("total_vat"),
        spark_round(spark_sum("net_amount_paid"), 2).alias("total_net_paid"),
        spark_round(spark_sum("amount_paid"),     2).alias("total_paid"),
    )
    .orderBy("provider")
    .show(truncate=False)
)

print("\nSite type and payment type breakdown:")
unified.groupBy("site_type","payment_type").count().orderBy("site_type").show()

# COMMAND ----------

# ─── Before / after comparison ────────────────────────────────────────────────

print("VendPark Bronze (NET basis):")
spark.table(f"{BRONZE_DATABASE}.bronze_vendpark") \
    .select("txn_ref","net_charge","discount_code","discounted_net","vat_amount","amount_paid") \
    .show(5, truncate=False)

print("VendPark Silver (GROSS basis, normalised):")
spark.table(f"{SILVER_DATABASE}.silver_vendpark") \
    .select("transaction_id","gross_charge","discount_code","discount_amount",
            "discounted_gross","vat_amount","net_amount_paid","amount_paid") \
    .show(5, truncate=False)

print("EasyEntry Bronze (£ symbols, integer vat_pct, ticket numbers):")
spark.table(f"{BRONZE_DATABASE}.bronze_easyentry") \
    .select("id","site_name","ticket_number","full_price","discounted_price","vat_pct","charged") \
    .show(5, truncate=False)

print("EasyEntry Silver (clean doubles, paid_duration for barrierless):")
spark.table(f"{SILVER_DATABASE}.silver_easyentry") \
    .select("transaction_id","site_name","site_type","ticket_number",
            "gross_charge","discount_amount","net_amount_paid","amount_paid",
            "duration_minutes","paid_duration_minutes") \
    .show(10, truncate=False)