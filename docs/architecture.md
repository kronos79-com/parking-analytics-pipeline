# Pipeline Architecture

## Overview

This pipeline follows the **Medallion Architecture** pattern, a data design approach
advocated by Databricks in which data flows through three progressively refined layers:
Bronze, Silver, and Gold. Each layer has a single, clearly defined responsibility.
No layer skips ahead or borrows work from another.

The pipeline processes parking transaction data from three equipment providers into
a unified analytical dataset. The core engineering challenge is that each provider
uses different schemas, different date formats, different site identification schemes,
and different payment processing arrangements, yet the business needs a single
coherent view of all ten car parks.

---

## Layer Responsibilities

### Bronze: Trust the Source

Bronze is a trust boundary. Its only job is to land data exactly as it arrived,
faults and all. Nothing is cleaned, cast, or interpreted at this layer.

Two audit columns are appended to every row:

- `_ingested_at` : timestamp the record was loaded into the pipeline
- `_source_file` : filename the record originated from

These two columns are the first thing an engineer checks during a production incident.
They provide an unambiguous answer to "when did this data arrive and where did it come from?"

**Why preserve dirty data deliberately?**

If Silver cleans data and something goes wrong downstream, you need to be able to
prove what the source actually sent you. Bronze is that proof. It is the audit trail.
Cleaning Bronze data would be like filing a court document and then editing it.

### Silver: Make Data Trustworthy

Silver reads from Bronze and applies three operations in sequence:

**Cleanse** - fix provider-specific data quality problems. Each problem is handled
explicitly with a documented decision. For example, null charge amounts are defaulted
to 0.0 rather than dropped, because a null charge on a real transaction is more likely
a feed issue than a genuinely free parking session. The decision is documented in code.

**Conform** - rename columns so all three providers share one canonical schema.
`transaction_id`, `txn_ref`, and `id` all become `transaction_id`. `charge_amount`,
`amount_charged`, and `charge` all become `charge_amount`. After Silver, the
downstream layers have no awareness that three providers ever existed.

**Enrich** - join each transaction to the site map reference table to resolve
provider-specific site identifiers to a `canonical_site_id`. ParkTech's `PT-1001`,
VendPark's `riverside-retail`, and EasyEntry's UUID all resolve to `SITE-001`
through `SITE-010`.

**Why write both individual and unified Silver tables?**

Individual provider tables (`silver_parktech`, `silver_vendpark`, `silver_easyentry`)
exist for debugging. If a Gold aggregate looks wrong, you can isolate which provider
contributed the problem in one query without scanning the entire unified dataset.
The unified table (`silver_transactions_unified`) is what Gold reads from.

### Gold: Answer Business Questions

Gold reads exclusively from the unified Silver table and produces aggregated,
business-ready outputs. No cleansing happens at Gold. If something looks wrong
in Gold, the fix belongs in Silver.

Gold tables are named for the question they answer, not the data they contain:

| Table | Question answered |
|---|---|
| `gold_revenue_by_site` | How much did each car park earn today? |
| `gold_revenue_by_acquirer` | How much should each acquirer settle to us? |
| `gold_payment_method_breakdown` | What payment methods are our customers using? |
| `gold_discount_analysis` | What is each discount scheme costing us? |
| `gold_monthly_summary` | How did each site perform this month vs last month? |

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        SOURCE DATA                          │
│                                                             │
│  parktech_march_2025.csv   (26,295 rows, Stripe PSP)        │
│  vendpark_march_2025.csv   (16,052 rows, ADVAM PSP)         │
│  easyentry_march_2025.csv  ( 8,421 rows, SIX PSP)           │
│  site_map.csv              (    10 rows, reference)         │
└──────────────────────────┬──────────────────────────────────┘
                           │  01_ingest_bronze.py
                           │  Reads CSV with explicit schema
                           │  Appends _ingested_at, _source_file
                           │  Writes Delta, mode=overwrite
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER                           │
│                   (parking_bronze)                          │
│                                                             │
│  bronze_parktech       26,295 rows  dirty data preserved    │
│  bronze_vendpark       16,052 rows  dirty data preserved    │
│  bronze_easyentry       8,421 rows  dirty data preserved    │
│  bronze_site_map           10 rows  reference table         │
└──────────────────────────┬──────────────────────────────────┘
                           │  02_transform_silver.py
                           │  Cleanses per-provider problems
                           │  Deduplicates ParkTech (256 rows removed)
                           │  Conforms to canonical schema
                           │  Joins to site map for canonical_site_id
                           │  Unions all three providers
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                           │
│                   (parking_silver)                          │
│                                                             │
│  silver_parktech       26,039 rows  cleansed, conformed     │
│  silver_vendpark       16,052 rows  cleansed, conformed     │
│  silver_easyentry       8,421 rows  cleansed, conformed     │
│  silver_transactions_unified                                │
│                        50,512 rows  unified canonical schema│
└──────────────────────────┬──────────────────────────────────┘
                           │  03_build_gold.py
                           │  Aggregates by site, acquirer,
                           │  payment method, discount, month
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                       GOLD LAYER                            │
│                    (parking_gold)                           │
│                                                             │
│  gold_revenue_by_site            310 rows  daily per site   │
│  gold_revenue_by_acquirer        186 rows  daily per PSP    │
│  gold_payment_method_breakdown    53 rows  monthly per site │
│  gold_discount_analysis            5 rows  monthly summary  │
│  gold_monthly_summary             10 rows  one per site     │
└─────────────────────────────────────────────────────────────┘
```

---

## Payment Processing Architecture

Understanding the PSP and acquirer relationship is important for the
settlement reconciliation use case.

A **PSP (Payment Service Provider)** handles the technical processing
of a card transaction: authentication, tokenisation, routing.

An **acquirer** is the bank or financial institution that settles the
funds into the merchant's account. The acquirer is determined by the
card scheme used, not the PSP.

```
Customer card
      │
      ▼
   Terminal
      │
      ▼
    PSP  ──────────────────────────────────────────┐
(ADVAM / SIX / Stripe)                             │
      │                                            │
      │  Routes based on card scheme               │
      ▼                                            ▼
  Worldpay                                       AMEX
(Visa, Mastercard,                          (AMEX cards only,
  Maestro cards)                             direct merchant
                                              agreement)
```

**Stripe** is both PSP and acquirer. It handles all card schemes
including AMEX through its own acquiring infrastructure.

**Cash transactions** bypass the PSP entirely. The terminal records
the transaction locally. `psp = N/A` and `acquirer = N/A`.

---

## Delta Lake Transaction Log

Every write to every Delta table is recorded in a transaction log.
This provides three capabilities used in this pipeline:

**Time travel** - query any previous version of a table:
```python
spark.read.format("delta").option("versionAsOf", 0).load(path)
```

**Audit history** - see every write with timestamp, user, and row counts:
```python
spark.sql("DESCRIBE HISTORY delta.`/path/to/table`").show()
```

**Rollback** - restore a table to any previous version if a bad
load corrupts data. In production this is the recovery mechanism
when a Silver cleansing bug makes it through to Gold before being caught.

---

## FinOps Note

In a production environment each notebook would run as a **job cluster**
rather than an interactive all-purpose cluster. Job clusters spin up
for the duration of the run and terminate immediately on completion.
For a pipeline of this scale, that reduces compute cost by approximately
70% compared to a persistent interactive cluster that idles between runs.

The Bronze ingestion step is the most compute-efficient as it performs
minimal transformation. The Silver deduplication window function is
the most expensive operation in the pipeline as it requires a full
shuffle across the ParkTech dataset.

---

## Month on Month Comparison

The `gold_monthly_summary` table is deliberately structured for MoM analysis.
Every row carries a `month_label` column (`2025-03`, `2026-03` etc.).

To produce a side-by-side comparison once March 2026 data is available:

```python
mar_2025 = spark.table("parking_gold.gold_monthly_summary").filter(col("month_label") == "2025-03")
mar_2026 = spark.table("parking_gold.gold_monthly_summary").filter(col("month_label") == "2026-03")

comparison = (
    mar_2025.alias("y1")
    .join(mar_2026.alias("y2"), on="canonical_site_id")
    .select(
        col("y1.site_name"),
        col("y1.total_revenue").alias("revenue_2025"),
        col("y2.total_revenue").alias("revenue_2026"),
        (col("y2.total_revenue") - col("y1.total_revenue")).alias("revenue_delta"),
    )
)
```

To generate March 2026 data, update `MARCH_DATES` in
`scripts/generate_datasets_v2.py` to use `datetime(2026,3,d)`,
set `MONTH_LABEL = "2026-03"` in `03_build_gold.py`, and
re-run all three notebooks.
