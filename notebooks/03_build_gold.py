# Databricks notebook source
# ─── NOTEBOOK: 03_build_gold.py ──────────────────────────────────────────────
#
# PURPOSE
# -------
# Read from the unified Silver table and produce business-ready Gold tables.
# No cleansing happens here. If something looks wrong in Gold, the fix
# belongs in Silver.
#
# GOLD TABLES PRODUCED
# --------------------
#   gold_revenue_by_site         : daily revenue KPIs per site
#   gold_revenue_by_acquirer     : daily settlement volume per acquirer/PSP
#   gold_payment_method_breakdown: transaction mix by payment method per site
#   gold_discount_analysis       : revenue and VAT impact per discount scheme
#   gold_financial_summary       : gross, net, VAT, discount totals by provider
#   gold_monthly_summary         : one row per site for MoM comparison
#
# FINANCIAL FIELDS IN GOLD
# ------------------------
# All Gold aggregations use the canonical Silver financial schema:
#   gross_charge     : full undiscounted tariff, VAT inclusive
#   discount_amount  : value of discount granted, VAT inclusive
#   discounted_gross : gross after discount
#   vat_amount       : VAT element of amount paid
#   net_amount_paid  : what the customer paid, ex-VAT (for finance reporting)
#   amount_paid      : what the customer paid, VAT inclusive
#
# FINOPS NOTE
# -----------
# In production this notebook would run on a job cluster, not an interactive
# cluster. For batch workloads this reduces compute cost by approximately 70%.
# The Silver -> Gold step is the most aggregation-heavy and benefits most
# from Photon vectorised execution.
#
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer
# MAGIC ## Parking Analytics Pipeline
# MAGIC Produces six business-ready aggregated tables from the unified Silver layer.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg,
    round as spark_round, max as spark_max,
    to_date, lit, when, countDistinct,
    coalesce
)
from pyspark.sql.types import IntegerType
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

SILVER_DATABASE = "parking_silver"
GOLD_DATABASE   = "parking_gold"
MONTH_LABEL     = "2025-03"

print(f"Reading from : {SILVER_DATABASE}")
print(f"Writing to   : {GOLD_DATABASE}")
print(f"Month label  : {MONTH_LABEL}")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}")
spark.sql(f"USE {GOLD_DATABASE}")
print(f"Using database: {GOLD_DATABASE}")

# COMMAND ----------

# ─── Load Silver ──────────────────────────────────────────────────────────────
#
# transaction_date derived from transaction_time rather than entry_timestamp.
# transaction_time is present on every row including barrierless sites where
# entry_timestamp is null. This ensures all rows contribute to daily aggregations.

silver = (
    spark.table(f"{SILVER_DATABASE}.silver_transactions_unified")
    .withColumn(
        "transaction_date",
        to_date(col("transaction_time"))
    )
)

total = silver.count()
print(f"Silver rows loaded : {total:,}")

print("\nDate range (by transaction_time):")
silver.selectExpr(
    "min(transaction_date) as earliest",
    "max(transaction_date) as latest"
).show()

print("Row counts by site_type:")
silver.groupBy("site_type").count().orderBy("site_type").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1: Revenue by Site (Daily)
# MAGIC
# MAGIC One row per site per day. Core operational KPI table.
# MAGIC Includes gross, net, VAT, and discount totals alongside transaction counts.
# MAGIC Uses effective_duration - actual for barrier sites, paid for barrierless.

# COMMAND ----------

# Effective duration: actual where available, paid_duration for barrierless
silver_with_eff = silver.withColumn(
    "effective_duration",
    coalesce(col("duration_minutes"), col("paid_duration_minutes"))
)

gold_revenue_by_site = (
    silver_with_eff
    .groupBy(
        "transaction_date",
        "canonical_site_id",
        "site_name",
        "city",
        "capacity",
        "site_type",
        "provider",
    )
    .agg(
        count("transaction_id")                                     .alias("transaction_count"),
        spark_round(spark_sum("gross_charge"),     2)               .alias("total_gross_charge"),
        spark_round(spark_sum("discount_amount"),  2)               .alias("total_discounts"),
        spark_round(spark_sum("discounted_gross"), 2)               .alias("total_discounted_gross"),
        spark_round(spark_sum("vat_amount"),       2)               .alias("total_vat"),
        spark_round(spark_sum("net_amount_paid"),  2)               .alias("total_net_paid"),
        spark_round(spark_sum("amount_paid"),      2)               .alias("total_amount_paid"),
        spark_round(avg("gross_charge"),           2)               .alias("avg_gross_charge"),
        spark_round(avg("effective_duration"),     1)               .alias("avg_duration_minutes"),
        spark_sum(when(col("amount_paid") == 0,    1).otherwise(0)) .alias("free_transactions"),
        spark_sum(when(col("discount_code") != "", 1).otherwise(0)) .alias("discounted_transactions"),
        spark_sum(when(col("payment_method") == "CASH", 1).otherwise(0)) .alias("cash_transactions"),
        countDistinct("vrm")                                        .alias("unique_vrms"),
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
# MAGIC One row per PSP/acquirer combination per day.
# MAGIC Settlement reconciliation table.
# MAGIC net_amount_paid is the field to submit to financial packages.

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
        spark_round(spark_sum("gross_charge"),   2)     .alias("total_gross_charge"),
        spark_round(spark_sum("vat_amount"),     2)     .alias("total_vat"),
        spark_round(spark_sum("net_amount_paid"),2)     .alias("total_net_paid"),
        spark_round(spark_sum("amount_paid"),    2)     .alias("total_amount_paid"),
        spark_round(avg("amount_paid"),          2)     .alias("avg_amount_paid"),
        countDistinct("canonical_site_id")              .alias("sites_count"),
    )
    .orderBy("transaction_date", "acquirer")
)

count_rba = gold_revenue_by_acquirer.count()
print(f"gold_revenue_by_acquirer : {count_rba:,} rows")
gold_revenue_by_acquirer.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3: Payment Method Breakdown (by Site, Monthly)
# MAGIC
# MAGIC Transaction counts and revenue by payment method per site.
# MAGIC Answers: what proportion of customers use each payment method?

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
        "site_type",
        "provider",
        "payment_method",
    )
    .agg(
        count("transaction_id")                         .alias("transaction_count"),
        spark_round(spark_sum("amount_paid"),    2)     .alias("total_amount_paid"),
        spark_round(spark_sum("net_amount_paid"),2)     .alias("total_net_paid"),
        spark_round(avg("amount_paid"),          2)     .alias("avg_amount_paid"),
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
# MAGIC Revenue and VAT impact of each discount scheme.
# MAGIC Shows gross value waived, net value waived, and VAT not collected.
# MAGIC Critical for evaluating whether each scheme is financially justified.

# COMMAND ----------

gold_discount_analysis = (
    silver
    .withColumn(
        "discount_label",
        when(
            col("discount_code").isNull() | (col("discount_code") == ""),
            "NO_DISCOUNT"
        ).otherwise(col("discount_code"))
    )
    .groupBy("discount_label", "product_id")
    .agg(
        count("transaction_id")                             .alias("transaction_count"),
        spark_round(spark_sum("gross_charge"),       2)     .alias("gross_value_before_discount"),
        spark_round(spark_sum("discount_amount"),    2)     .alias("total_discount_gross"),
        spark_round(spark_sum("discounted_gross"),   2)     .alias("total_discounted_gross"),
        spark_round(spark_sum("vat_amount"),         2)     .alias("vat_collected"),
        spark_round(spark_sum("net_amount_paid"),    2)     .alias("net_revenue_collected"),
        spark_round(spark_sum("amount_paid"),        2)     .alias("gross_revenue_collected"),
        spark_round(avg("amount_paid"),              2)     .alias("avg_amount_paid"),
        countDistinct("canonical_site_id")                  .alias("sites_used_at"),
    )
    .withColumn(
        "discount_pct_of_gross",
        spark_round(
            col("total_discount_gross") / col("gross_value_before_discount") * 100, 1
        )
    )
    .orderBy("transaction_count", ascending=False)
)

count_da = gold_discount_analysis.count()
print(f"gold_discount_analysis : {count_da:,} rows")
gold_discount_analysis.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 5: Financial Summary by Provider (Monthly)
# MAGIC
# MAGIC Total gross, net, VAT, and discount values per provider for the month.
# MAGIC This table confirms the VendPark NET -> GROSS normalisation produced
# MAGIC consistent financial totals across all three providers.
# MAGIC Net values are what gets submitted to financial/accounting packages.

# COMMAND ----------

gold_financial_summary = (
    silver
    .groupBy("provider", "psp")
    .agg(
        count("transaction_id")                             .alias("transaction_count"),
        spark_round(spark_sum("gross_charge"),       2)     .alias("total_gross_charge"),
        spark_round(spark_sum("net_charge"),         2)     .alias("total_net_charge"),
        spark_round(spark_sum("discount_amount"),    2)     .alias("total_discount_gross"),
        spark_round(spark_sum("discounted_gross"),   2)     .alias("total_discounted_gross"),
        spark_round(spark_sum("vat_amount"),         2)     .alias("total_vat_collected"),
        spark_round(spark_sum("net_amount_paid"),    2)     .alias("total_net_paid"),
        spark_round(spark_sum("amount_paid"),        2)     .alias("total_gross_paid"),
    )
    .withColumn(
        "effective_vat_rate_pct",
        spark_round(
            col("total_vat_collected") / col("total_net_paid") * 100, 2
        )
    )
    .orderBy("provider")
)

count_fs = gold_financial_summary.count()
print(f"gold_financial_summary : {count_fs:,} rows")
gold_financial_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 6: Monthly Summary (per Site)
# MAGIC
# MAGIC One row per site for the entire month.
# MAGIC Structured for March 2025 vs March 2026 MoM comparison.
# MAGIC Includes financial totals on both gross and net basis.

# COMMAND ----------

daily_by_site = (
    silver
    .withColumn("transaction_date", to_date(col("transaction_time")))
    .groupBy("canonical_site_id", "transaction_date")
    .agg(
        spark_sum("amount_paid").alias("daily_revenue"),
        count("transaction_id").alias("daily_transactions"),
    )
)

busiest_day = (
    daily_by_site
    .groupBy("canonical_site_id")
    .agg(
        spark_round(spark_max("daily_revenue"),    2).alias("peak_daily_revenue"),
        spark_max("daily_transactions")             .alias("peak_daily_transactions"),
    )
)

avg_daily = (
    daily_by_site
    .groupBy("canonical_site_id")
    .agg(
        spark_round(avg("daily_revenue"),          2).alias("avg_daily_revenue"),
        spark_round(avg("daily_transactions"),     1).alias("avg_daily_transactions"),
    )
)

silver_with_eff2 = silver.withColumn(
    "effective_duration",
    coalesce(col("duration_minutes"), col("paid_duration_minutes"))
)

gold_monthly_summary = (
    silver_with_eff2
    .groupBy(
        "canonical_site_id",
        "site_name",
        "city",
        "capacity",
        "site_type",
        "provider",
    )
    .agg(
        count("transaction_id")                                         .alias("total_transactions"),
        spark_round(spark_sum("gross_charge"),           2)             .alias("total_gross_charge"),
        spark_round(spark_sum("discount_amount"),        2)             .alias("total_discounts"),
        spark_round(spark_sum("vat_amount"),             2)             .alias("total_vat"),
        spark_round(spark_sum("net_amount_paid"),        2)             .alias("total_net_paid"),
        spark_round(spark_sum("amount_paid"),            2)             .alias("total_amount_paid"),
        spark_round(avg("gross_charge"),                 2)             .alias("avg_gross_charge"),
        spark_round(avg("effective_duration"),           1)             .alias("avg_duration_minutes"),

        # Cash mix
        spark_round(
            spark_sum(when(col("payment_method") == "CASH", 1).otherwise(0)) * 100.0
            / count("transaction_id"), 1
        )                                                               .alias("cash_pct"),

        # AMEX mix
        spark_round(
            spark_sum(when(col("acquirer") == "AMEX", 1).otherwise(0)) * 100.0
            / count("transaction_id"), 1
        )                                                               .alias("amex_pct"),

        # Discount mix
        spark_round(
            spark_sum(when(
                col("discount_code").isNotNull() & (col("discount_code") != ""), 1
            ).otherwise(0)) * 100.0 / count("transaction_id"), 1
        )                                                               .alias("discounted_pct"),

        # VRM capture rate (ANPR sites only - null for EasyEntry)
        spark_round(
            spark_sum(when(
                col("vrm").isNotNull() & (col("vrm") != ""), 1
            ).otherwise(0)) * 100.0 / count("transaction_id"), 1
        )                                                               .alias("vrm_capture_pct"),

        countDistinct("transaction_date")                               .alias("active_days"),
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
    ("gold_financial_summary",        gold_financial_summary),
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

print("\nVerification - Gold table row counts:")
print("-" * 55)
for table_name, _ in gold_tables:
    c = spark.table(f"{GOLD_DATABASE}.{table_name}").count()
    print(f"  {table_name:<40} {c:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Insight Queries

# COMMAND ----------

# ─── Top 5 sites by total revenue ────────────────────────────────────────────

print("Top 5 sites by total revenue - March 2025:")
(
    spark.table(f"{GOLD_DATABASE}.gold_monthly_summary")
    .select(
        "canonical_site_id", "site_name", "site_type",
        "total_transactions", "total_gross_charge",
        "total_discounts", "total_vat", "total_net_paid",
        "avg_gross_charge", "vrm_capture_pct",
    )
    .orderBy("total_gross_charge", ascending=False)
    .limit(5)
    .show(truncate=False)
)

# COMMAND ----------

# ─── Monthly settlement summary by acquirer ──────────────────────────────────

print("Monthly settlement by acquirer - what each acquirer owes us:")
(
    spark.table(f"{GOLD_DATABASE}.gold_revenue_by_acquirer")
    .groupBy("acquirer", "psp")
    .agg(
        spark_round(spark_sum("total_gross_charge"), 2).alias("monthly_gross"),
        spark_round(spark_sum("total_vat"),          2).alias("monthly_vat"),
        spark_round(spark_sum("total_net_paid"),     2).alias("monthly_net"),
        spark_round(spark_sum("total_amount_paid"),  2).alias("monthly_gross_paid"),
        spark_sum("transaction_count")                 .alias("monthly_transactions"),
    )
    .orderBy("monthly_gross_paid", ascending=False)
    .show(truncate=False)
)

# COMMAND ----------

# ─── Financial summary - gross vs net by provider ────────────────────────────

print("Financial summary by provider - gross vs net:")
(
    spark.table(f"{GOLD_DATABASE}.gold_financial_summary")
    .select(
        "provider", "psp", "transaction_count",
        "total_gross_charge", "total_net_charge",
        "total_discount_gross", "total_vat_collected",
        "total_net_paid", "total_gross_paid",
        "effective_vat_rate_pct",
    )
    .show(truncate=False)
)

# COMMAND ----------

# ─── Discount impact ─────────────────────────────────────────────────────────

print("Discount scheme impact - March 2025:")
(
    spark.table(f"{GOLD_DATABASE}.gold_discount_analysis")
    .select(
        "discount_label", "product_id", "transaction_count",
        "gross_value_before_discount", "total_discount_gross",
        "vat_collected", "net_revenue_collected",
        "gross_revenue_collected", "discount_pct_of_gross",
        "sites_used_at",
    )
    .show(truncate=False)
)

# COMMAND ----------

# ─── Payment method mix ───────────────────────────────────────────────────────

print("Payment method mix - all sites combined:")
(
    spark.table(f"{GOLD_DATABASE}.gold_payment_method_breakdown")
    .groupBy("payment_method")
    .agg(
        spark_sum("transaction_count")                 .alias("total_transactions"),
        spark_round(spark_sum("total_amount_paid"), 2) .alias("total_gross_paid"),
        spark_round(spark_sum("total_net_paid"),    2) .alias("total_net_paid"),
    )
    .orderBy("total_transactions", ascending=False)
    .show(truncate=False)
)

# COMMAND ----------

# ─── Month on Month comparison preview ───────────────────────────────────────
#
# Ready to run once March 2026 data is loaded.
# To generate March 2026: update MARCH_DATES in generate_datasets_v4.py
# to datetime(2026,3,d), set MONTH_LABEL = "2026-03" above, re-run all
# three notebooks.

print("Monthly summary - March 2025 (re-run after March 2026 load for MoM):")
(
    spark.table(f"{GOLD_DATABASE}.gold_monthly_summary")
    .select(
        "month_label", "canonical_site_id", "site_name", "site_type",
        "total_transactions", "total_gross_charge",
        "total_discounts", "total_vat", "total_net_paid",
        "avg_daily_revenue", "peak_daily_revenue",
        "cash_pct", "amex_pct", "discounted_pct", "vrm_capture_pct",
    )
    .orderBy("canonical_site_id")
    .show(truncate=False)
)