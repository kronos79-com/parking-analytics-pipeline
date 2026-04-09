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
# SOURCE SCHEMAS (v4 datasets)
# ----------------------------
# ParkTech (barrier_anpr, pay_on_exit):
#   transaction_id, site_id, site_name, vrm,
#   entry_timestamp, exit_timestamp, transaction_time, duration_minutes,
#   payment_method, card_scheme,
#   gross_charge, discount_code, product_id, discount_amount,
#   discounted_charge, vat_rate, vat_amount, amount_paid,
#   psp, acquirer, psp_reference
#
# VendPark (barrier_anpr, pay_on_exit, NET reporting):
#   car_park_slug, car_park_name, txn_ref, vrm,
#   arrival, departure, transaction_time, stay_minutes,
#   payment_type, card_scheme,
#   net_charge, discount_code, product_id,
#   discounted_net, vat_rate, vat_amount, amount_paid,
#   psp, acquirer, psp_reference
#
# EasyEntry (mixed: barrier_ticket and barrierless):
#   site_uuid, site_name, id, ticket_number,
#   entry_time, exit_time, transaction_time, duration_mins,
#   payment_method, card_scheme,
#   full_price, discount_code, product_id,
#   discounted_price, vat_pct, tax_amount, charged,
#   psp, acquirer, psp_reference
#
# Site map:
#   canonical_site_id, site_name, city, capacity,
#   site_type, payment_type, open_hour, close_hour, ticket_prefix,
#   parktech_id, vendpark_slug, easyentry_uuid
#
# KEY DESIGN DECISIONS
# --------------------
# - All columns ingested as StringType regardless of their eventual type.
#   Casting at ingestion would silently null or error on dirty values.
#   Bronze preserves exactly what the source sent. Silver does all casting.
# - enforceSchema=True rejects files that do not match the declared schema.
# - mode=FAILFAST aborts on any malformed row rather than silently dropping.
# - Writes use saveAsTable for Unity Catalog compatibility.
#
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC ## Parking Analytics Pipeline
# MAGIC Loads raw source CSV files into Delta tables without any transformation.
# MAGIC Re-running performs a full reload of all providers.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

SOURCE_BASE_PATH = "/FileStore/tables"
BRONZE_DATABASE  = "parking_bronze"

print(f"Source path  : {SOURCE_BASE_PATH}")
print(f"Database     : {BRONZE_DATABASE}")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DATABASE}")
spark.sql(f"USE {BRONZE_DATABASE}")
print(f"Using database: {BRONZE_DATABASE}")

# COMMAND ----------

# ─── Source definitions ───────────────────────────────────────────────────────
#
# All columns are StringType at Bronze. Dirty values (nulls, £ symbols,
# negative numbers, mixed-case strings) are preserved exactly as received.
# Silver handles all casting and cleansing.

def str_field(name):
    return StructField(name, StringType(), True)

SOURCES = [
    {
        "provider":   "ParkTech",
        "filename":   "parktech_march_2025.csv",
        "table_name": "bronze_parktech",
        "schema": StructType([
            str_field("transaction_id"),
            str_field("site_id"),
            str_field("site_name"),
            str_field("vrm"),
            str_field("entry_timestamp"),
            str_field("exit_timestamp"),
            str_field("transaction_time"),
            str_field("duration_minutes"),
            str_field("payment_method"),
            str_field("card_scheme"),
            str_field("gross_charge"),        # occasionally null (dirty)
            str_field("discount_code"),
            str_field("product_id"),
            str_field("discount_amount"),
            str_field("discounted_charge"),
            str_field("vat_rate"),
            str_field("vat_amount"),
            str_field("amount_paid"),
            str_field("psp"),
            str_field("acquirer"),
            str_field("psp_reference"),
        ]),
    },
    {
        "provider":   "VendPark",
        "filename":   "vendpark_march_2025.csv",
        "table_name": "bronze_vendpark",
        "schema": StructType([
            str_field("car_park_slug"),
            str_field("car_park_name"),
            str_field("txn_ref"),
            str_field("vrm"),
            str_field("arrival"),             # UK date format dd/MM/yyyy HH:mm
            str_field("departure"),           # UK date format, some before arrival
            str_field("transaction_time"),    # UK date format
            str_field("stay_minutes"),        # occasionally null
            str_field("payment_type"),        # mixed case problem
            str_field("card_scheme"),
            str_field("net_charge"),          # NET reporting (ex-VAT)
            str_field("discount_code"),
            str_field("product_id"),
            str_field("discounted_net"),      # NET reporting
            str_field("vat_rate"),
            str_field("vat_amount"),
            str_field("amount_paid"),
            str_field("psp"),
            str_field("acquirer"),
            str_field("psp_reference"),
        ]),
    },
    {
        "provider":   "EasyEntry",
        "filename":   "easyentry_march_2025.csv",
        "table_name": "bronze_easyentry",
        "schema": StructType([
            str_field("site_uuid"),
            str_field("site_name"),
            str_field("id"),
            str_field("ticket_number"),       # populated for barrier_ticket sites only
            str_field("entry_time"),          # empty for barrierless sites
            str_field("exit_time"),           # empty for barrierless, occasionally missing
            str_field("transaction_time"),
            str_field("duration_mins"),       # empty for barrierless, occasionally negative
            str_field("payment_method"),
            str_field("card_scheme"),
            str_field("full_price"),          # £ symbol present (e.g. "£5.00")
            str_field("discount_code"),
            str_field("product_id"),
            str_field("discounted_price"),    # £ symbol present
            str_field("vat_pct"),             # integer string "20" not decimal
            str_field("tax_amount"),          # £ symbol present
            str_field("charged"),             # £ symbol present
            str_field("psp"),
            str_field("acquirer"),
            str_field("psp_reference"),
        ]),
    },
    {
        "provider":   "SiteMap",
        "filename":   "site_map.csv",
        "table_name": "bronze_site_map",
        "schema": StructType([
            str_field("canonical_site_id"),
            str_field("site_name"),
            str_field("city"),
            str_field("capacity"),
            str_field("site_type"),           # barrier_anpr / barrier_ticket / barrierless
            str_field("payment_type"),        # pay_on_exit / prepay
            str_field("open_hour"),           # populated for time-bounded sites only
            str_field("close_hour"),          # populated for time-bounded sites only
            str_field("ticket_prefix"),       # populated for barrier_ticket sites only
            str_field("parktech_id"),
            str_field("vendpark_slug"),
            str_field("easyentry_uuid"),
        ]),
    },
]

# COMMAND ----------

def ingest_to_bronze(source: dict) -> None:
    """
    Reads a single source CSV into a Bronze Delta table.

    Uses saveAsTable for Unity Catalog compatibility.
    Overwrites on every run - Bronze is a full reload pattern.
    Incremental loading is a Silver concern once data is trusted.
    """
    file_path  = f"{SOURCE_BASE_PATH}/{source['filename']}"
    table_name = source["table_name"]

    print(f"\n[{source['provider']}] Reading from : {file_path}")

    try:
        df = (
            spark.read
            .format("csv")
            .option("header",        "true")
            .option("enforceSchema", "true")
            .option("mode",          "FAILFAST")
            .schema(source["schema"])
            .load(file_path)
        )

        df = (
            df
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", lit(source["filename"]))
        )

        row_count = df.count()
        print(f"[{source['provider']}] Rows read    : {row_count:,}")

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{BRONZE_DATABASE}.{table_name}")
        )

        print(f"[{source['provider']}] Table        : {BRONZE_DATABASE}.{table_name}  DONE")

    except Exception as e:
        print(f"[{source['provider']}] FAILED: {e}")
        raise

# COMMAND ----------

run_start = datetime.now()
print(f"Bronze ingestion started : {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

for source in SOURCES:
    ingest_to_bronze(source)

elapsed = (datetime.now() - run_start).seconds
print("\n" + "=" * 60)
print(f"Bronze ingestion complete : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Elapsed                   : {elapsed}s")

# COMMAND ----------

print("\nVerification - Bronze table row counts:")
print("-" * 50)
for source in SOURCES:
    table = f"{BRONZE_DATABASE}.{source['table_name']}"
    count = spark.table(table).count()
    print(f"  {source['table_name']:<30} {count:>8,} rows")

# COMMAND ----------

# ─── Data peek ────────────────────────────────────────────────────────────────
#
# All dirty data should be visible here. This is intentional.
# - ParkTech : occasional null gross_charge, UNKNOWN payment_method,
#              duplicate transaction_ids, VRM with ~5-10% null
# - VendPark : mixed-case payment_type, UK dates, VP- transaction refs,
#              NET financial values, VRM with ~5-10% null
# - EasyEntry: £ symbol on price fields, integer vat_pct,
#              ticket_number on barrier sites, empty entry/exit on barrierless

print("\nParkTech sample (financial fields, VRM):")
spark.table(f"{BRONZE_DATABASE}.bronze_parktech") \
    .select("transaction_id","vrm","entry_timestamp","payment_method",
            "gross_charge","discount_code","discount_amount",
            "discounted_charge","vat_rate","vat_amount","amount_paid") \
    .show(5, truncate=False)

print("\nVendPark sample (NET financials, mixed-case payment_type, UK dates):")
spark.table(f"{BRONZE_DATABASE}.bronze_vendpark") \
    .select("txn_ref","vrm","arrival","departure","payment_type",
            "net_charge","discount_code","discounted_net","vat_amount","amount_paid") \
    .show(5, truncate=False)

print("\nEasyEntry barrier_ticket sample (ticket_number, £ prices):")
spark.table(f"{BRONZE_DATABASE}.bronze_easyentry") \
    .filter("ticket_number != ''") \
    .select("id","site_name","ticket_number","entry_time","exit_time",
            "full_price","discounted_price","vat_pct","tax_amount","charged") \
    .show(5, truncate=False)

print("\nEasyEntry barrierless sample (no entry/exit/duration):")
spark.table(f"{BRONZE_DATABASE}.bronze_easyentry") \
    .filter("entry_time = ''") \
    .select("id","site_name","transaction_time",
            "full_price","discounted_price","vat_pct","charged") \
    .show(5, truncate=False)

print("\nSite map:")
spark.table(f"{BRONZE_DATABASE}.bronze_site_map").show(truncate=False)

# COMMAND ----------

# ─── Delta transaction log ────────────────────────────────────────────────────

print("\nDelta transaction log - ParkTech Bronze:")
spark.sql(f"DESCRIBE HISTORY {BRONZE_DATABASE}.bronze_parktech").show(3, truncate=False)