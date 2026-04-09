# Databricks notebook source

# ─── NOTEBOOK: 03_build_gold.py ──────────────────────────────────────────────
#
# PURPOSE
# -------
# Read from the unified Silver table and produce five business-ready Gold
# tables. No cleansing happens here. If something looks wrong in Gold,
# the fix belongs in Silver, not here.
#
# GOLD TABLES PRODUCED
# --------------------
#   gold_revenue_by_site         : daily revenue and transaction KPIs per site
#   gold_revenue_by_acquirer     : daily settlement volume per acquirer
#   gold_payment_method_breakdown: transaction mix by payment method per site
#   gold_discount_analysis       : revenue impact of each discount code
#   gold_monthly_summary         : one row per site for the full month
#                                  (designed for MoM comparison)
#
# DESIGN PRINCIPLES
# -----------------
# - Gold reads exclusively from Silver. Never from Bronze or raw CSV.
# - All aggregations are deterministic and documented.
# - Null handling is explicit: nulls in charge_amount were resolved in Silver
#   so Gold can safely sum without coalesce guards.
# - The monthly summary table is deliberately structured for a simple JOIN
#   between March 2025 and March 2026 once the second dataset is generated.
#
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer
# MAGIC ## Parking Analytics Pipeline
# MAGIC Produces five business-ready aggregated tables from the unified Silver layer.

# COMMAND ----------

# ─── Imports ──────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, round as spark_round,
    max as spark_max, min as spark_min, date_trunc,
    to_date, lit, when, countDistinct
)
from pyspark.sql.types import DoubleType
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ─── Configuration ────────────────────────────────────────────────────────────

SILVER_DATABASE = "parking_silver"
GOLD_DATABASE   = "parking_gold"
MONTH_LABEL     = "2025-03"       # used in monthly summary for MoM joins

print(f"Reading from : {SILVER_DATABASE}")
print(f"Writing to   : {GOLD_DATABASE}")
print(f"Month label  : {MONTH_LABEL}")

# COMMAND ----------

# ─── Create Gold database ─────────────────────────────────────────────────────

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}")
spark.sql(f"USE {GOLD_DATABASE}")
print(f"Using database: {GOLD_DATABASE}")

# COMMAND ----------

# ─── Load Silver unified table ────────────────────────────────────────────────
#
# This is the only table Gold reads from.
# We add a transaction_date column derived from entry_timestamp which is
# used as the grouping key in all daily aggregations.

silver = (
    spark.table(f"{SILVER_DATABASE}.silver_transactions_unified")
    .withColumn(
        "transaction_date",
        to_date(col("entry_timestamp"))
    )
)

total = silver.count()
print(f"Silver rows loaded : {total:,}")

print("\nDate range:")
silver.selectExpr(
    "min(transaction_date) as earliest",
    "max(transaction_date) as latest"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1: Revenue by Site (Daily)
# MAGIC
# MAGIC One row per site per day. Core operational KPI table.
# MAGIC Answers: how much did each car park earn today?

# COMMAND ----------

gold_revenue_by_site = (
    silver
    .groupBy(
        "transaction_date",
        "canonical_site_id",
        "site_name",
        "city",
        "capacity",
        "provider",
    )
    .agg(
        count("transaction_id")                         .alias("transaction_count"),
        spark_round(spark_sum("charge_amount"), 2)      .alias("total_revenue"),
        spark_round(avg("charge_amount"), 2)            .alias("avg_charge"),
        spark_round(avg("duration_minutes"), 1)         .alias("avg_duration_minutes"),
        spark_sum(
            when(col("charge_amount") == 0, 1).otherwise(0)
        )                                               .alias("free_transactions"),
        spark_sum(
            when(col("discount_code").isNotNull(), 1).otherwise(0)
        )                                               .alias("discounted_transactions"),
        spark_sum(
            when(col("payment_method") == "CASH", 1).otherwise(0)
        )                                               .alias("cash_transactions"),
    )
    .withColumn(
        "occupancy_rate_pct",
        spark_round((col("transaction_count") / col("capacity")) * 100, 1)
    )
    .orderBy("transaction_date", "canonical_site_id")
)

count_rbs = gold_revenue_by_site.count()
print(f"gold_revenue_by_site : {count_rbs:,} rows")
gold_revenue_by_site.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 2: Revenue by Acquirer (Daily)
# MAGIC
# MAGIC One row per acquirer per day. Settlement reconciliation table.
# MAGIC Answers: how much should Worldpay settle to us today?

# COMMAND ----------

gold_revenue_by_acquirer = (
    silver
    .groupBy(
        "transaction_date",
        "psp",
        "acquirer",
    )
    .agg(
        count("transaction_id")                         .alias("transaction_count"),
        spark_round(spark_sum("charge_amount"), 2)      .alias("total_revenue"),
        spark_round(avg("charge_amount"), 2)            .alias("avg_charge"),
        countDistinct("canonical_site_id")              .alias("sites_count"),
    )
    .orderBy("transaction_date", "psp")
)

count_rba = gold_revenue_by_acquirer.count()
print(f"gold_revenue_by_acquirer : {count_rba:,} rows")
gold_revenue_by_acquirer.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3: Payment Method Breakdown (by Site)
# MAGIC
# MAGIC One row per site per payment method for the full month.
# MAGIC Answers: what proportion of customers use contactless vs cash vs app?

# COMMAND ----------

site_totals = (
    silver
    .groupBy("canonical_site_id")
    .agg(count("transaction_id").alias("site_total"))
)

gold_payment_method_breakdown = (
    silver
    .groupBy(
        "canonical_site_id",
        "site_name",
        "provider",
        "payment_method",
    )
    .agg(
        count("transaction_id")                         .alias("transaction_count"),
        spark_round(spark_sum("charge_amount"), 2)      .alias("total_revenue"),
        spark_round(avg("charge_amount"), 2)            .alias("avg_charge"),
    )
    .join(site_totals, on="canonical_site_id", how="left")
    .withColumn(
        "pct_of_site_transactions",
        spark_round((col("transaction_count") / col("site_total")) * 100, 1)
    )
    .drop("site_total")
    .orderBy("canonical_site_id", "transaction_count")
)

count_pmb = gold_payment_method_breakdown.count()
print(f"gold_payment_method_breakdown : {count_pmb:,} rows")
gold_payment_method_breakdown.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 4: Discount Analysis (Full Month)
# MAGIC
# MAGIC Revenue impact of each discount code across all sites.
# MAGIC Answers: how much revenue did each discount scheme cost us this month?

# COMMAND ----------

gold_discount_analysis = (
    silver
    .withColumn(
        "discount_label",
        when(col("discount_code").isNull(), "NO_DISCOUNT")
        .otherwise(col("discount_code"))
    )
    .groupBy(
        "discount_label",
        "product_id",
    )
    .agg(
        count("transaction_id")                         .alias("transaction_count"),
        spark_round(spark_sum("charge_amount"), 2)      .alias("revenue_collected"),
        spark_round(avg("charge_amount"), 2)            .alias("avg_charge"),
        spark_round(avg("duration_minutes"), 1)         .alias("avg_duration_minutes"),
        countDistinct("canonical_site_id")              .alias("sites_used_at"),
    )
    .orderBy("transaction_count", ascending=False)
)

count_da = gold_discount_analysis.count()
print(f"gold_discount_analysis : {count_da:,} rows")
gold_discount_analysis.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 5: Monthly Summary (per Site)
# MAGIC
# MAGIC One row per site for the entire month.
# MAGIC Structured for March 2025 vs March 2026 MoM comparison.

# COMMAND ----------

daily_by_site = (
    silver
    .groupBy("canonical_site_id", "transaction_date")
    .agg(
        spark_sum("charge_amount").alias("daily_revenue"),
        count("transaction_id").alias("daily_transactions"),
    )
)

busiest_day = (
    daily_by_site
    .groupBy("canonical_site_id")
    .agg(
        spark_round(spark_max("daily_revenue"), 2)      .alias("peak_daily_revenue"),
        spark_max("daily_transactions")                 .alias("peak_daily_transactions"),
    )
)

avg_daily = (
    daily_by_site
    .groupBy("canonical_site_id")
    .agg(
        spark_round(avg("daily_revenue"), 2)            .alias("avg_daily_revenue"),
        spark_round(avg("daily_transactions"), 1)       .alias("avg_daily_transactions"),
    )
)

gold_monthly_summary = (
    silver
    .groupBy(
        "canonical_site_id",
        "site_name",
        "city",
        "capacity",
        "provider",
    )
    .agg(
        count("transaction_id")                         .alias("total_transactions"),
        spark_round(spark_sum("charge_amount"), 2)      .alias("total_revenue"),
        spark_round(avg("charge_amount"), 2)            .alias("avg_charge_per_transaction"),
        spark_round(avg("duration_minutes"), 1)         .alias("avg_duration_minutes"),
        spark_round(
            spark_sum(when(col("payment_method") == "CASH", 1).otherwise(0)) * 100.0
            / count("transaction_id"), 1
        )                                               .alias("cash_pct"),
        spark_round(
            spark_sum(when(col("acquirer") == "AMEX", 1).otherwise(0)) * 100.0
            / count("transaction_id"), 1
        )                                               .alias("amex_pct"),
        spark_round(
            spark_sum(when(col("discount_code").isNotNull(), 1).otherwise(0)) * 100.0
            / count("transaction_id"), 1
        )                                               .alias("discounted_pct"),
        countDistinct("transaction_date")               .alias("active_days"),
    )
    .join(busiest_day, on="canonical_site_id", how="left")
    .join(avg_daily,   on="canonical_site_id", how="left")
    .withColumn("month_label", lit(MONTH_LABEL))
    .orderBy("canonical_site_id")
)

count_ms = gold_monthly_summary.count()
print(f"gold_monthly_summary : {count_ms:,} rows  (one per site)")
gold_monthly_summary.show(truncate=False)

# COMMAND ----------

# ─── Write all Gold tables ────────────────────────────────────────────────────

print("\nWriting Gold tables...")
run_start = datetime.now()

gold_tables = [
    ("gold_revenue_by_site",          gold_revenue_by_site),
    ("gold_revenue_by_acquirer",      gold_revenue_by_acquirer),
    ("gold_payment_method_breakdown", gold_payment_method_breakdown),
    ("gold_discount_analysis",        gold_discount_analysis),
    ("gold_monthly_summary",          gold_monthly_summary),
]

for table_name, df in gold_tables:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_DATABASE}.{table_name}")
    )
    print(f"  Written : {GOLD_DATABASE}.{table_name}")

elapsed = (datetime.now() - run_start).seconds
print(f"\nGold layer complete in {elapsed}s")

# COMMAND ----------

# ─── Verification ─────────────────────────────────────────────────────────────

print("\nVerification - Gold table row counts:")
print("-" * 55)

for table_name, _ in gold_tables:
    c = spark.table(f"{GOLD_DATABASE}.{table_name}").count()
    print(f"  {table_name:<40} {c:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Insight Queries

# COMMAND ----------

# ─── Top 5 revenue generating sites for the month ────────────────────────────

print("Top 5 sites by total revenue - March 2025:")
(
    spark.table(f"{GOLD_DATABASE}.gold_monthly_summary")
    .select(
        "canonical_site_id",
        "site_name",
        "total_transactions",
        "total_revenue",
        "avg_charge_per_transaction",
        "avg_duration_minutes",
    )
    .orderBy("total_revenue", ascending=False)
    .limit(5)
    .show(truncate=False)
)

# COMMAND ----------

# ─── Acquirer settlement summary for the month ───────────────────────────────

print("Monthly settlement summary by acquirer:")
(
    spark.table(f"{GOLD_DATABASE}.gold_revenue_by_acquirer")
    .groupBy("acquirer", "psp")
    .agg(
        spark_round(spark_sum("total_revenue"), 2).alias("monthly_revenue"),
        spark_sum("transaction_count").alias("monthly_transactions"),
    )
    .orderBy("monthly_revenue", ascending=False)
    .show(truncate=False)
)

# COMMAND ----------

# ─── Discount revenue impact ──────────────────────────────────────────────────

print("Discount scheme impact - March 2025:")
(
    spark.table(f"{GOLD_DATABASE}.gold_discount_analysis")
    .select(
        "discount_label",
        "product_id",
        "transaction_count",
        "revenue_collected",
        "avg_duration_minutes",
        "sites_used_at",
    )
    .show(truncate=False)
)

# COMMAND ----------

# ─── Payment method mix across all sites ─────────────────────────────────────

print("Payment method mix - all sites combined:")
(
    spark.table(f"{GOLD_DATABASE}.gold_payment_method_breakdown")
    .groupBy("payment_method")
    .agg(
        spark_sum("transaction_count").alias("total_transactions"),
        spark_round(spark_sum("total_revenue"), 2).alias("total_revenue"),
    )
    .orderBy("total_transactions", ascending=False)
    .show(truncate=False)
)

# COMMAND ----------

# ─── Month on Month comparison preview ───────────────────────────────────────
#
# This query is ready to run once March 2026 data is loaded.
# Generate March 2026 data using generate_datasets_v2.py with
# MARCH_DATES updated to 2026, run all three notebooks, then re-run.

print("Monthly summary - March 2025:")
print("Re-run after loading March 2026 for full MoM comparison.")
print()

(
    spark.table(f"{GOLD_DATABASE}.gold_monthly_summary")
    .select(
        "month_label",
        "canonical_site_id",
        "site_name",
        "total_transactions",
        "total_revenue",
        "avg_daily_revenue",
        "peak_daily_revenue",
        "cash_pct",
        "amex_pct",
        "discounted_pct",
    )
    .orderBy("canonical_site_id")
    .show(truncate=False)
)
