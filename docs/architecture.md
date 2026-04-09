# Pipeline Architecture

## Overview

This pipeline implements the **Medallion Architecture** pattern across parking transaction
data from three equipment providers. Data flows through Bronze, Silver, and Gold layers,
each with a single clearly defined responsibility. No layer skips ahead or borrows work
from another.

The central engineering challenge is that three providers send fundamentally different
data: different schemas, different date formats, different site identification schemes,
different financial reporting bases, different payment processing arrangements, and
different physical site technologies. The business needs a single coherent view across
all ten car parks. This pipeline provides that.

---

## Layer Responsibilities

### Bronze: Preserve Exactly What Arrived

Bronze is a trust boundary. Its only job is to land data exactly as the source sent it,
faults included. Nothing is cleaned, cast, or interpreted at this layer.

Two audit columns are appended to every row:

- `_ingested_at` : timestamp the record was loaded into the pipeline
- `_source_file` : filename the record originated from

All source columns are ingested as `StringType` regardless of their eventual type.
Casting at ingestion would silently null or error on dirty values such as empty strings,
currency symbols, or out-of-range numbers. By preserving raw strings at Bronze, every
data quality problem remains visible and traceable. Silver handles all casting with
explicit, documented logic.

**Why preserve dirty data deliberately?**

If Silver cleans data and something goes wrong downstream, you need to prove what the
source actually sent. Bronze is that proof. It is the audit trail. Cleaning Bronze
would be like filing a legal document and then editing it after the fact.

### Silver: Make Data Trustworthy

Silver reads from Bronze and applies four operations in sequence.

**Cleanse** fixes provider-specific data quality problems. Every problem is handled
explicitly with documented logic. For example, null `gross_charge` rows in ParkTech
have all financial fields set to null rather than defaulted to zero. Zero implies a
free transaction. Null is honest about an unknown value.

**Conform** renames all columns to a single canonical schema so all three providers
look identical downstream. `transaction_id`, `txn_ref`, and `id` all become
`transaction_id`. `gross_charge`, `net_charge`, and `full_price` all become
`gross_charge` after normalisation.

**Normalise** converts VendPark's NET financial reporting to GROSS so all three
providers share the same financial basis. The conversion follows the HMRC-compliant
approach: convert to gross first, extract VAT by the formula
`vat = gross - (gross / VAT_DIVISOR)`, then derive net by subtraction. This avoids
the floating point rounding drift that occurs when converting independently at
multiple points in a calculation chain.

**Enrich** joins each transaction to the site map reference table to resolve
provider-specific site identifiers to a `canonical_site_id` and adds site metadata
including `site_type`, `payment_type`, `capacity`, and opening hours.

Silver writes both individual provider tables and a unified table. The individual
tables exist for debugging: if a Gold aggregate looks wrong you can isolate which
provider contributed the problem in one query without scanning 50,000 rows.

### Gold: Answer Business Questions

Gold reads exclusively from the unified Silver table and produces aggregated,
business-ready outputs. No cleansing happens at Gold. If something looks wrong
in Gold, the fix belongs in Silver.

Gold tables are named for the question they answer, not the data they contain.
`transaction_date` in Gold is derived from `transaction_time` rather than
`entry_timestamp` because `entry_timestamp` is null for barrierless sites.
Using `transaction_time` ensures every row contributes to daily aggregations
regardless of site technology.

---

## Data Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                          SOURCE DATA                             │
│                                                                  │
│  parktech_march_2025.csv    26,289 rows  Stripe PSP  GROSS       │
│  vendpark_march_2025.csv    15,982 rows  ADVAM PSP   NET         │
│  easyentry_march_2025.csv    8,376 rows  SIX PSP     GROSS+£     │
│  site_map.csv                   10 rows  reference               │
└───────────────────────────────┬──────────────────────────────────┘
                                │  01_ingest_bronze.py
                                │  All columns as StringType
                                │  Appends audit columns
                                │  saveAsTable (Unity Catalog)
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                              │
│                      (parking_bronze)                            │
│                                                                  │
│  bronze_parktech       26,289 rows  dirty data intact            │
│  bronze_vendpark       15,982 rows  dirty data intact            │
│  bronze_easyentry       8,376 rows  dirty data intact            │
│  bronze_site_map           10 rows  reference table              │
└───────────────────────────────┬──────────────────────────────────┘
                                │  02_transform_silver.py
                                │  Per-provider cleansing
                                │  VendPark NET -> GROSS conversion
                                │  Canonical schema conforming
                                │  Site map join -> canonical_site_id
                                │  Union all three providers
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER                              │
│                      (parking_silver)                            │
│                                                                  │
│  silver_parktech       ~25,800 rows  cleansed, conformed         │
│  silver_vendpark        15,982 rows  cleansed, NET->GROSS        │
│  silver_easyentry        8,376 rows  cleansed, £ stripped        │
│  silver_transactions_unified                                     │
│                        ~50,158 rows  unified canonical schema    │
└───────────────────────────────┬──────────────────────────────────┘
                                │  03_build_gold.py
                                │  Six business aggregations
                                │  transaction_date from transaction_time
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                               │
│                       (parking_gold)                             │
│                                                                  │
│  gold_revenue_by_site            ~310 rows  daily per site       │
│  gold_revenue_by_acquirer        ~186 rows  daily per PSP        │
│  gold_payment_method_breakdown    ~53 rows  monthly per site     │
│  gold_discount_analysis             5 rows  monthly all sites    │
│  gold_financial_summary             6 rows  monthly per provider │
│  gold_monthly_summary              10 rows  one per site         │
└──────────────────────────────────────────────────────────────────┘
```

---

## Site Technology Architecture

Three distinct site configurations exist across the ten car parks, each with different
data characteristics and different Silver handling requirements.

### barrier_anpr

Six sites use ANPR cameras at entry and exit barriers. Every transaction has an
`entry_timestamp`, `exit_timestamp`, and `duration_minutes` derived from the ANPR
read. A `vrm` field is populated at a 90-95% capture rate. When a plate read fails
the VRM is null. `duration_minutes` is the authoritative duration for these sites.
`paid_duration_minutes` is null.

### barrier_ticket

Two EasyEntry sites (Seafront Long Stay and Harbour View) use barrier-controlled entry
with physical tickets. The barrier issues a numbered ticket on entry (`TKT-SLS-xxxxxx`
or `TKT-HV-xxxxxx`). The ticket number is encoded and matched at the payment machine.
Entry and exit timestamps are recorded via the barrier system. No VRM is captured.
Payment is on exit. `duration_minutes` is populated. `paid_duration_minutes` is null.

### barrierless

Two EasyEntry sites (The Lanes Short Stay and North Street Surface) have no barriers
and no ANPR. Customers pay at a machine on arrival for a chosen tariff band. No entry
or exit timestamps are recorded. No VRM is captured. No ticket is issued.

These sites have operating hours constraints:
- The Lanes Short Stay: 08:00 to 18:00
- North Street Surface: 06:00 to 20:00

`entry_timestamp`, `exit_timestamp`, and `duration_minutes` are null for all
barrierless transactions. `paid_duration_minutes` is the maximum minutes of the gross
tariff band the customer purchased. This reflects the entitlement granted before any
discount is applied: a customer receiving PROMO-20 on the 60-minute band still bought
60 minutes of parking. `transaction_time` is the only timestamp present and is
constrained to the site's operating hours.

---

## Financial Architecture

### Reporting basis by provider

VendPark reports NET (ex-VAT). ParkTech and EasyEntry report GROSS (VAT inclusive).
Silver normalises all three to the same canonical financial schema before any Gold
aggregation occurs.

### VAT extraction method

The HMRC-compliant method for extracting VAT from a gross price is:

```
vat_amount = gross - (gross / VAT_DIVISOR)
net        = gross - vat_amount
```

Where `VAT_DIVISOR = 1 + VAT_RATE`. This produces net by subtraction from a single
rounded VAT figure, avoiding floating point drift from two independent rounding
operations. `VAT_RATE` is defined once at the top of the Silver notebook. Changing
it to `0.175` for the pre-2011 rate or any future rate requires one edit.

### VendPark NET to GROSS conversion

```
gross_charge     = net_charge_src * VAT_DIVISOR       (rounded to 2dp)
discount_amount  = gross_charge * (1 - discounted_net / net_charge)
discounted_gross = gross_charge - discount_amount
vat_amount       = discounted_gross - (discounted_gross / VAT_DIVISOR)
net_amount_paid  = discounted_gross - vat_amount
```

Converting gross first and deriving all other values from that single gross figure
eliminates the 0.01 rounding error that occurs when converting both `net_charge` and
`discounted_net` independently to gross and then trying to reconcile them.

### Free transactions

BLUE-BADGE and SEASON-Q1 transactions have `amount_paid = 0` and `vat_amount = 0`.
`gross_charge` retains the full tariff value. This is deliberate: finance needs to
report the value of concessions granted, not just revenue collected. A zero
`gross_charge` would hide the financial impact of these schemes.

### Null gross_charge rows

Approximately 2% of ParkTech transactions have a null `gross_charge` due to a
source feed defect. Silver sets all financial fields to null on these rows rather
than defaulting to zero. Zero would silently understate revenue in Gold aggregations.
The data quality check reports these rows explicitly as `Null gross_charge (dirty
source rows)` so they are visible and trackable.

---

## Payment Processing Architecture

```
Customer card
      │
      ▼
   Terminal (ParkTech / VendPark / EasyEntry equipment)
      │
      ▼
    PSP (processes the transaction technically)
    ├── Stripe     (ParkTech only)
    ├── ADVAM      (VendPark only)
    └── SIX        (EasyEntry only)
      │
      │  Routes on card scheme
      ├──────────────────────────────────────────┐
      ▼                                          ▼
  Worldpay                                    AMEX
  (Visa, Mastercard, Maestro)            (AMEX cards only)
  ADVAM and SIX only                     ADVAM and SIX only
      │                                          │
      └──────────────┬───────────────────────────┘
                     ▼
              Stripe acquires all
              card types including
              AMEX for ParkTech
```

Cash transactions have `psp = N/A` and `acquirer = N/A`. The terminal records the
transaction locally. No card network is involved and no settlement file is generated.
Cash revenue must be reconciled separately against physical cash collections.

---

## Delta Lake

All tables are written as Delta format throughout the pipeline. This provides three
capabilities that plain Parquet cannot:

**ACID transactions** mean a failed write never leaves a partial or corrupt table.
If the Silver notebook fails halfway through writing the unified table, the previous
version remains intact and queryable.

**Time travel** means every write is versioned. Any previous version can be queried:

```python
spark.read.format("delta").option("versionAsOf", 0).load(path)
```

**Transaction log** provides a full audit trail of every write with timestamp,
operation type, user, row counts, and cluster ID:

```python
spark.sql("DESCRIBE HISTORY parking_bronze.bronze_parktech").show()
```

---

## Data Quality Checks

Silver runs nine automated checks after every execution:

| Check | What it validates |
|---|---|
| Null gross_charge | Count of dirty source rows (expect ~506) |
| UNKNOWN payment_method | ParkTech normalisation (expect 0) |
| Mixed-case payment_method | VendPark normalisation (expect 0) |
| Negative duration_minutes | EasyEntry normalisation (expect 0) |
| Missing canonical_site_id | Site map join completeness (expect 0) |
| Barrierless with duration | Duration nullification (expect 0) |
| Barrier with paid_duration | paid_duration exclusivity (expect 0) |
| net * 1.2 != amount_paid | Financial consistency (expect 0) |
| discount + discounted != gross | Financial consistency with 1p tolerance (expect 0) |

---

## FinOps Note

In production each notebook would run as a **job cluster** rather than an interactive
all-purpose cluster. Job clusters spin up for the duration of the run and terminate
immediately on completion. For a pipeline of this scale this reduces compute cost by
approximately 70% compared to a persistent interactive cluster that idles between runs.

The Silver deduplication window function on ParkTech is the most compute-intensive
operation in the pipeline as it requires a full shuffle across the dataset. In
production this would be a candidate for partitioning by `site_id` to reduce shuffle
volume.

The pipeline ran on a Photon-enabled Databricks runtime (15.4 LTS), which provides
vectorised query execution for significant performance gains on aggregation-heavy
Gold operations.

---

## Month on Month Comparison

The `gold_monthly_summary` table carries a `month_label` column (`2025-03`, `2026-03`
etc.). Once March 2026 data is loaded, a direct JOIN produces side-by-side performance
metrics:

```python
mar25 = spark.table("parking_gold.gold_monthly_summary").filter(col("month_label") == "2025-03")
mar26 = spark.table("parking_gold.gold_monthly_summary").filter(col("month_label") == "2026-03")

comparison = (
    mar25.alias("y1")
    .join(mar26.alias("y2"), on="canonical_site_id")
    .select(
        col("y1.site_name"),
        col("y1.total_gross_charge").alias("gross_2025"),
        col("y2.total_gross_charge").alias("gross_2026"),
        (col("y2.total_gross_charge") - col("y1.total_gross_charge")).alias("gross_delta"),
        col("y1.total_net_paid").alias("net_2025"),
        col("y2.total_net_paid").alias("net_2026"),
    )
)
```

To generate March 2026 data, update `MARCH_DATES` in `scripts/generate_datasets_v4.py`
to `datetime(2026,3,d)`, set `MONTH_LABEL = "2026-03"` in `03_build_gold.py`, and
re-run all three notebooks in sequence.
