# Databricks notebook source

# ─── NOTEBOOK: 01_ingest_bronze.py ───────────────────────────────────────────
#
# PURPOSE
# -------
# Ingest raw source files from all three equipment providers into Bronze Delta
# tables. No transformations are applied. Data is landed exactly as received,
# with two audit columns appended:
#   _ingested_at  : timestamp this record was loaded
#   _source_file  : filename the record originated from
#
# ARCHITECTURE
# ------------
# This notebook represents the Bronze layer of the Medallion architecture.
# Bronze = raw, immutable, append-friendly. Think of it as the audit trail.
# Downstream Silver notebooks read from here and apply all cleansing logic.
#
# MEDALLION LAYERS
# ----------------
#   Bronze  (this notebook) : raw landing zone, no transforms
#   Silver  (02_...)        : cleansed, normalised, conformed
#   Gold    (03_...)        : aggregated analytical output, business-ready
#
# DELTA LAKE
# ----------
# Tables are written as Delta format which provides:
#   - ACID transactions     : failed writes never leave partial data
#   - Time travel           : query any previous version with VERSION AS OF
#   - Schema enforcement    : malformed source files are rejected, not silently loaded
#
# USAGE
# -----
# Run cells sequentially. Widget at top allows selective reload of one provider
# or all three. Safe to re-run: tables are overwritten in full each execution
# (append mode with deduplication is a Silver concern, not Bronze).
#
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC ## Parking Analytics Pipeline
# MAGIC Loads raw source CSV files into Delta tables without any transformation.
# MAGIC Re-running this notebook performs a full reload of the selected provider(s).

# COMMAND ----------

# ─── Imports ─────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ─── Configuration ────────────────────────────────────────────────────────────
#
# SOURCE_BASE_PATH : where your CSV files live in DBFS or cloud storage.
#                   In Databricks Community Edition, upload files via the
#                   Data tab and reference them as /FileStore/tables/
#
# BRONZE_BASE_PATH : where Delta tables will be written.
#
# Adjust these paths to match your workspace setup.

SOURCE_BASE_PATH = "/FileStore/tables"
BRONZE_BASE_PATH = "/delta/bronze"

# Database (schema) that will hold our Bronze tables.
# Using a dedicated database keeps things tidy in the metastore.
BRONZE_DATABASE = "parking_bronze"

print(f"Source path  : {SOURCE_BASE_PATH}")
print(f"Bronze path  : {BRONZE_BASE_PATH}")
print(f"Database     : {BRONZE_DATABASE}")

# COMMAND ----------

# ─── Create database if it does not exist ────────────────────────────────────

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DATABASE}")
spark.sql(f"USE {BRONZE_DATABASE}")
print(f"Using database: {BRONZE_DATABASE}")

# COMMAND ----------

# ─── Source file definitions ──────────────────────────────────────────────────
#
# Each entry defines one source feed. Adding a new provider in future is as
# simple as adding a new entry to this list - the ingestion logic is generic.
#
# Fields:
#   provider      : human-readable provider name (used in logging)
#   filename      : CSV filename in the sources folder
#   table_name    : target Bronze Delta table name
#   schema        : explicit schema - never infer schema from raw sources.
#                   Inference can silently miscast columns and is non-deterministic
#                   across Spark versions. Explicit schemas are self-documenting.

SOURCES = [
    {
        "provider"   : "ParkTech",
        "filename"   : "parktech_march_2025.csv",
        "table_name" : "bronze_parktech",
        "schema"     : StructType([
            StructField("transaction_id",   StringType(),  True),
            StructField("site_id",          StringType(),  True),
            StructField("site_name",        StringType(),  True),
            StructField("entry_timestamp",  StringType(),  True),  # kept as string - Silver will cast
            StructField("exit_timestamp",   StringType(),  True),
            StructField("duration_minutes", StringType(),  True),  # string to survive dirty nulls
            StructField("vehicle_type",     StringType(),  True),
            StructField("payment_method",   StringType(),  True),
            StructField("card_scheme",      StringType(),  True),
            StructField("charge_amount",    StringType(),  True),  # string - Silver strips £ and casts
            StructField("discount_code",    StringType(),  True),
            StructField("product_id",       StringType(),  True),
            StructField("psp",              StringType(),  True),
            StructField("acquirer",         StringType(),  True),
            StructField("psp_reference",    StringType(),  True),
        ]),
    },
    {
        "provider"   : "VendPark",
        "filename"   : "vendpark_march_2025.csv",
        "table_name" : "bronze_vendpark",
        "schema"     : StructType([
            StructField("txn_ref",          StringType(),  True),
            StructField("car_park_slug",    StringType(),  True),
            StructField("car_park_name",    StringType(),  True),
            StructField("arrival",          StringType(),  True),
            StructField("departure",        StringType(),  True),
            StructField("stay_minutes",     StringType(),  True),
            StructField("vehicle",          StringType(),  True),
            StructField("payment_type",     StringType(),  True),
            StructField("card_scheme",      StringType(),  True),
            StructField("amount_charged",   StringType(),  True),
            StructField("discount_code",    StringType(),  True),
            StructField("product_id",       StringType(),  True),
            StructField("psp",              StringType(),  True),
            StructField("acquirer",         StringType(),  True),
            StructField("psp_reference",    StringType(),  True),
        ]),
    },
    {
        "provider"   : "EasyEntry",
        "filename"   : "easyentry_march_2025.csv",
        "table_name" : "bronze_easyentry",
        "schema"     : StructType([
            StructField("id",               StringType(),  True),
            StructField("site_uuid",        StringType(),  True),
            StructField("site_name",        StringType(),  True),
            StructField("entry_time",       StringType(),  True),
            StructField("exit_time",        StringType(),  True),
            StructField("duration_mins",    StringType(),  True),
            StructField("vehicle_class",    StringType(),  True),
            StructField("payment_method",   StringType(),  True),
            StructField("card_scheme",      StringType(),  True),
            StructField("charge",           StringType(),  True),
            StructField("discount_code",    StringType(),  True),
            StructField("product_id",       StringType(),  True),
            StructField("psp",              StringType(),  True),
            StructField("acquirer",         StringType(),  True),
            StructField("psp_reference",    StringType(),  True),
        ]),
    },
    {
        "provider"   : "SiteMap",
        "filename"   : "site_map.csv",
        "table_name" : "bronze_site_map",
        "schema"     : StructType([
            StructField("canonical_site_id", StringType(), True),
            StructField("site_name",         StringType(), True),
            StructField("city",              StringType(), True),
            StructField("capacity",          StringType(), True),  # string for safety
            StructField("parktech_id",       StringType(), True),
            StructField("vendpark_slug",     StringType(), True),
            StructField("easyentry_uuid",    StringType(), True),
        ]),
    },
]

# COMMAND ----------

# ─── Ingestion function ───────────────────────────────────────────────────────

def ingest_to_bronze(source: dict) -> None:
    """
    Reads a single source CSV into a Bronze Delta table.

    Design decisions:
    - header=True        : first row is column names
    - enforceSchema=True : rejects files that do not match the declared schema
                           rather than silently loading garbage
    - mode="FAILFAST"    : aborts on any malformed row rather than silently
                           dropping it. In production you would catch this
                           exception and raise an alert.
    - overwrite          : Bronze is a full reload pattern. Incremental loading
                           (mergeSchema, MERGE INTO) is a Silver concern once
                           data is trusted.
    """

    file_path = f"{SOURCE_BASE_PATH}/{source['filename']}"
    table_name = source["table_name"]
    delta_path = f"{BRONZE_BASE_PATH}/{table_name}"

    print(f"\n[{source['provider']}] Reading from : {file_path}")

    try:
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("enforceSchema", "true")
            .option("mode", "FAILFAST")
            .schema(source["schema"])
            .load(file_path)
        )

        # Append audit columns - these are pipeline metadata, not source data
        df = (
            df
            .withColumn("_ingested_at",  current_timestamp())
            .withColumn("_source_file",  lit(source["filename"]))
        )

        row_count = df.count()
        print(f"[{source['provider']}] Rows read    : {row_count:,}")

        # Write as a managed Unity Catalog table
        # Unity Catalog handles storage location automatically
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{BRONZE_DATABASE}.{table_name}")
        )

        print(f"[{source['provider']}] Written to  : {delta_path}")
        print(f"[{source['provider']}] Table       : {BRONZE_DATABASE}.{table_name}  DONE")

    except Exception as e:
        print(f"[{source['provider']}] FAILED: {e}")
        raise


# COMMAND ----------

# ─── Run ingestion for all sources ───────────────────────────────────────────

run_start = datetime.now()
print(f"Bronze ingestion started : {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

for source in SOURCES:
    ingest_to_bronze(source)

run_end = datetime.now()
elapsed = (run_end - run_start).seconds

print("\n" + "=" * 60)
print(f"Bronze ingestion complete : {run_end.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Elapsed                   : {elapsed}s")

# COMMAND ----------

# ─── Verification ─────────────────────────────────────────────────────────────
#
# Always verify after loading. Never assume a write succeeded just because
# no exception was raised. Check the row counts match what you expect.

print("\nVerification - Bronze table row counts:")
print("-" * 45)

for source in SOURCES:
    table = f"{BRONZE_DATABASE}.{source['table_name']}"
    count = spark.table(table).count()
    print(f"  {source['table_name']:<30} {count:>8,} rows")

# COMMAND ----------

# ─── Quick data peek ─────────────────────────────────────────────────────────
#
# Eyeball the raw data to confirm it landed as expected.
# The dirty data should be visible here - that is intentional.
# Cleaning happens in Silver, not here.

print("\nSample rows - ParkTech (raw, faults visible):")
spark.table(f"{BRONZE_DATABASE}.bronze_parktech").show(5, truncate=False)

print("\nSample rows - VendPark (note UK date format and mixed-case payment_type):")
spark.table(f"{BRONZE_DATABASE}.bronze_vendpark").show(5, truncate=False)

print("\nSample rows - EasyEntry (note £ symbol in charge column):")
spark.table(f"{BRONZE_DATABASE}.bronze_easyentry").show(5, truncate=False)

print("\nSite map:")
spark.table(f"{BRONZE_DATABASE}.bronze_site_map").show(10, truncate=False)

# COMMAND ----------

# ─── Delta table history ─────────────────────────────────────────────────────
#
# Delta keeps a transaction log of every write. This is one of the key
# advantages over plain Parquet. You can audit every load and roll back
# to any previous version with:
#   spark.read.format("delta").option("versionAsOf", 0).load(path)

print("\nDelta transaction log - ParkTech Bronze:")
spark.sql(f"DESCRIBE HISTORY {BRONZE_DATABASE}.bronze_parktech").show(5, truncate=False)
