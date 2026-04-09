# Parking Analytics Pipeline

A production-style data engineering portfolio project built on **Databricks** and **Delta Lake**, implementing a full **Medallion architecture** (Bronze / Silver / Gold) across realistic multi-provider parking transaction data.

The pipeline ingests data from three equipment providers, each using different PSPs, acquirers, site technology, financial reporting bases, schema designs, date formats, and column ordering. It cleanses and conforms all three into a unified canonical schema and produces six business-ready Gold tables covering revenue, settlement reconciliation, payment mix, discount impact, financial reporting, and month-on-month performance.

---

## The Business Problem

A parking operator runs ten car parks across Brighton and Gatwick. Each site uses different equipment and technology:

- **ParkTech** processes payments via **Stripe** (PSP and acquirer), uses ANPR cameras for entry and exit, and reports financials on a **gross (VAT inclusive)** basis
- **VendPark** processes payments via **ADVAM** (PSP) routing to **Worldpay** or **AMEX** depending on card scheme, uses ANPR cameras, and reports financials on a **net (VAT exclusive)** basis
- **EasyEntry** processes payments via **SIX** (PSP) routing to **Worldpay** or **AMEX** depending on card scheme, with two barrier sites using physical tickets and two barrierless prepay sites

Without this pipeline, the operator cannot answer basic questions like "how much did we earn today?", "how much should Worldpay settle to us this month?", or "what is our VAT liability?" without manually reconciling three different spreadsheets in three different formats.

---

## Architecture

```
Raw CSVs (3 providers, 4 files)
            │
            ▼
┌───────────────────────┐
│     BRONZE LAYER      │  Raw landing zone. Data ingested exactly as
│   (parking_bronze)    │  received, faults and all. No transforms.
│    4 Delta tables     │  Audit columns: _ingested_at, _source_file.
└──────────┬────────────┘
           │
           ▼
┌───────────────────────┐
│     SILVER LAYER      │  Cleanse. Conform. Normalise. Enrich.
│   (parking_silver)    │  All providers unified to one canonical schema.
│    4 Delta tables     │  VendPark NET converted to GROSS basis.
└──────────┬────────────┘  Financial fields consistent across all providers.
           │
           ▼
┌───────────────────────┐
│      GOLD LAYER       │  Business-ready aggregations.
│    (parking_gold)     │  Six tables answering real operational questions.
│    6 Delta tables     │  Structured for MoM comparison.
└───────────────────────┘
```

---

## Site Technology

Each of the ten car parks uses one of three technology configurations:

| Type | Sites | Entry/Exit Data | VRM | Ticket | Payment |
|---|---|---|---|---|---|
| `barrier_anpr` | Castle Street, Central Station, Airport, Riverside, Westgate, Victoria Quarter | ANPR timestamps | Yes (90-95% capture) | No | Pay on exit |
| `barrier_ticket` | Seafront Long Stay, Harbour View | Barrier timestamps | No | Yes (TKT-SLS-xxxxxx / TKT-HV-xxxxxx) | Pay on exit |
| `barrierless` | The Lanes Short Stay, North Street Surface | None | No | No | Prepay |

**Barrierless sites** have opening hours constraints. The Lanes operates 08:00-18:00 and North Street operates 06:00-20:00. Customers pay for a tariff band on arrival. No entry or exit timestamps are recorded. `paid_duration_minutes` is the maximum of the tariff band purchased, derived from the gross charge before any discount. `duration_minutes` is null for these sites.

**ANPR capture rate** is 90-95% across ParkTech and VendPark sites. VRM is null for the remaining 5-10% of transactions. EasyEntry sites have no ANPR capability.

---

## Payment Architecture

```
Customer card
      │
      ▼
   Terminal
      │
      ▼
    PSP ────────────────────────────────────┐
(Stripe / ADVAM / SIX)                      │
      │                                     │
      │  Routes on card scheme              │
      ▼                                     ▼
  Worldpay                               AMEX
(Visa, Mastercard,                  (AMEX cards only,
  Maestro)                           direct merchant
                                      agreement)
```

| Provider | PSP | Non-AMEX Acquirer | AMEX Acquirer |
|---|---|---|---|
| ParkTech | Stripe | Stripe | Stripe |
| VendPark | ADVAM | Worldpay | AMEX |
| EasyEntry | SIX | Worldpay | AMEX |

Cash transactions carry `psp = N/A` and `acquirer = N/A`. No card network is involved and no PSP processes the transaction.

---

## Financial Reporting Basis

A critical complexity in this pipeline is that providers report financials differently:

| Provider | Reporting Basis | Silver Treatment |
|---|---|---|
| ParkTech | Gross (VAT inclusive) | Used directly |
| VendPark | Net (VAT exclusive) | Converted to gross: `net * 1.20` |
| EasyEntry | Gross (VAT inclusive) with £ symbol | £ stripped, cast to double |

Silver normalises all three to a consistent canonical financial schema. VAT is extracted using the formula `vat = gross - (gross / VAT_DIVISOR)` where `VAT_DIVISOR = 1 + VAT_RATE`. Net is then derived by subtraction rather than division, which is the HMRC-compliant method and avoids floating point rounding drift. `VAT_RATE` is defined once at the top of the Silver notebook and drives all calculations, making the pipeline adaptable to future VAT rate changes.

### Canonical financial fields (Silver and Gold)

| Field | Description |
|---|---|
| `gross_charge` | Full undiscounted tariff, VAT inclusive |
| `discount_amount` | Value of discount applied, VAT inclusive |
| `discounted_gross` | Gross charge after discount |
| `vat_rate` | VAT rate as decimal (0.20) |
| `vat_amount` | VAT element of amount paid |
| `net_charge` | Full undiscounted tariff, ex-VAT |
| `net_discount_amount` | Discount value, ex-VAT |
| `net_amount_paid` | Amount paid by customer, ex-VAT (for financial packages) |
| `amount_paid` | Amount paid by customer, VAT inclusive |

Free transactions (BLUE-BADGE, SEASON-Q1) retain `gross_charge` showing the full tariff value so finance can report the value of concessions granted. `amount_paid` and `vat_amount` are zero.

---

## Data Quality Problems Engineered Into Source Data

Silver is designed to resolve all of these deliberately:

| Provider | Problem | Silver Resolution |
|---|---|---|
| ParkTech | ~2% null `gross_charge` | All financial fields set to null (not zero - null is honest) |
| ParkTech | ~3% `UNKNOWN` payment method | Set to null |
| ParkTech | ~1% duplicate `transaction_id` | Deduplicate, keep earliest by `entry_timestamp` |
| VendPark | Mixed-case `payment_type` (`debit_card`, `Contactless`, `DEBIT_CARD`) | Normalise to `UPPER_SNAKE_CASE` |
| VendPark | UK date format (`01/03/2025 14:22`) | Parsed with explicit format pattern |
| VendPark | ~3% exit timestamp before entry | Exit and duration set to null |
| VendPark | ~2% null `stay_minutes` | Recalculated from timestamps where possible |
| VendPark | Net financial reporting | Converted to gross before canonical schema |
| EasyEntry | `£` symbol on all price fields (`£5.00`) | Stripped with regexp, cast to double |
| EasyEntry | `vat_pct` stored as integer string (`"20"`) | Cast and divided by 100 |
| EasyEntry | ~4% missing `exit_time` on barrier sites | Set to null |
| EasyEntry | ~2% negative `duration_mins` | Absolute value taken |

---

## Dataset

Three months of fictional parking transaction data generated by `scripts/generate_datasets_v4.py`, designed to replicate the complexity and variation of real multi-provider parking operations.

### Volume and realism

- March 2025: approximately **50,500 transactions** across 10 sites and 31 days
- Volume varies deterministically by car park profile (commuter, airport, retail, leisure, local) and day of week
- Cash rate varies by profile: 1% at airport, 15% at local surface car parks
- AMEX card rate approximately 12% across card transactions
- ~8% of transactions carry a discount code

### Discount codes

| Code | Product ID | Type | Effect |
|---|---|---|---|
| `BLUE-BADGE` | PROD-BB-001 | Free | 100% - disabled badge holder |
| `SEASON-Q1` | PROD-SQ-002 | Free | 100% - quarterly season ticket |
| `PROMO-20` | PROD-PR-003 | Percentage | 20% reduction |
| `VALID-CODE` | PROD-VC-004 | 2hr free | Deducts up to 2hr tariff band value |

---

## Repository Structure

```
parking-analytics-pipeline/
│
├── README.md
│
├── data/
│   └── sources/
│       ├── parktech_march_2025.csv
│       ├── vendpark_march_2025.csv
│       ├── easyentry_march_2025.csv
│       └── site_map.csv
│
├── notebooks/
│   ├── 01_ingest_bronze.py       # Raw ingestion to Delta
│   ├── 02_transform_silver.py    # Cleanse, conform, normalise financials
│   └── 03_build_gold.py          # Business aggregations
│
├── scripts/
│   └── generate_datasets_v4.py   # Synthetic data generator
│
└── docs/
    ├── architecture.md
    ├── 01_ingest_bronze_executed.html
    ├── 02_transform_silver_executed.html
    └── 03_build_gold_executed.html
```

---

## Gold Layer Outputs

### `gold_revenue_by_site`
Daily revenue, transaction count, gross charge, discount total, VAT, net paid, and an occupancy proxy per site. Includes `unique_vrms` for ANPR sites. Core operational KPI table.

### `gold_revenue_by_acquirer`
Daily settlement volume grouped by PSP and acquirer. Shows gross, VAT, net, and gross paid separately. Used to reconcile against Stripe, Worldpay, and AMEX settlement files. Cash transactions appear as `N/A / N/A`.

### `gold_payment_method_breakdown`
Transaction counts and revenue by payment method per site for the full month. Includes both gross and net totals and percentage of site transactions. Tells the operator whether to invest in more contactless terminals or maintain cash infrastructure.

### `gold_discount_analysis`
Full financial impact of each discount scheme. Shows gross value before discount, total discount granted, VAT collected, net revenue collected, and discount as a percentage of gross. Enables the operator to evaluate whether each scheme is financially justified.

### `gold_financial_summary`
Total gross, net, VAT, discount, and paid values per provider and PSP for the month. The primary table confirming VendPark NET to GROSS normalisation produced financially consistent results. Includes `effective_vat_rate_pct` which should be 20.00% (±0.05% rounding tolerance) for all providers.

### `gold_monthly_summary`
One row per site for the entire month. Includes financial totals on both gross and net basis, average and peak daily revenue, cash percentage, AMEX percentage, discount percentage, and VRM capture rate. Structured with a `month_label` column for direct MoM JOIN comparison once March 2026 data is generated.

---

## March 2025 Results

| Site | Type | Transactions | Gross Revenue | Net Paid | Avg Charge | Cash % | VRM % |
|---|---|---|---|---|---|---|---|
| Airport Express Park | barrier_anpr | ~7,800 | ~£155,000 | ~£129,000 | ~£19.84 | 0.8% | ~93% |
| Central Station NCP | barrier_anpr | ~9,900 | ~£102,000 | ~£85,000 | ~£10.31 | 2.0% | ~93% |
| Castle Street Multi-Storey | barrier_anpr | ~8,200 | ~£71,000 | ~£59,000 | ~£8.64 | 1.9% | ~93% |
| Westgate Shopping | barrier_anpr | ~6,600 | ~£25,000 | ~£21,000 | ~£3.82 | 4.0% | ~93% |
| Riverside Retail Park | barrier_anpr | ~5,700 | ~£22,000 | ~£18,000 | ~£3.85 | 4.3% | ~93% |
| Victoria Quarter | barrier_anpr | ~3,700 | ~£13,000 | ~£11,000 | ~£3.42 | 5.9% | ~93% |
| Seafront Long Stay | barrier_ticket | ~3,100 | ~£10,000 | ~£8,300 | ~£3.25 | 6.3% | 0% |
| The Lanes Short Stay | barrierless | ~2,300 | ~£5,800 | ~£4,800 | ~£2.54 | 7.1% | 0% |
| Harbour View | barrier_ticket | ~1,800 | ~£4,600 | ~£3,800 | ~£2.50 | 7.1% | 0% |
| North Street Surface | barrierless | ~1,200 | ~£1,600 | ~£1,300 | ~£1.40 | 15.4% | 0% |

---

## How to Run

### Prerequisites

- Databricks workspace (trial or full platform)
- Python 3.8+ with standard library only (for local data generation)

### Step 1: Generate source data

```bash
python scripts/generate_datasets_v4.py
```

Writes four CSV files to `data/sources/`. To generate March 2026 for MoM comparison, update `MARCH_DATES` to `datetime(2026,3,d)` and set `MONTH_LABEL = "2026-03"` in `03_build_gold.py`.

### Step 2: Upload to Databricks

Upload the four CSV files from `data/sources/` to `/FileStore/tables/` via **New > Add data**.

### Step 3: Run notebooks in sequence

```
01_ingest_bronze.py    →  reads CSVs, writes Bronze Delta tables
02_transform_silver.py →  reads Bronze, writes Silver Delta tables
03_build_gold.py       →  reads Silver, writes Gold Delta tables
```

Each notebook uses `mode("overwrite")` and is safe to re-run in full.

### Step 4: Query Gold tables

```python
spark.table("parking_gold.gold_monthly_summary").show()
spark.table("parking_gold.gold_financial_summary").show()
spark.table("parking_gold.gold_revenue_by_acquirer").show()
```

---

## Key Design Decisions

**All columns ingested as StringType at Bronze.** Casting at ingestion would silently null or error on dirty values. Preserving raw strings at Bronze means every data quality problem is visible, documented, and handled intentionally in Silver.

**Null rather than zero for missing gross_charge.** Zero implies a free transaction. A null `gross_charge` means the feed was corrupted and the charge is unknown. Defaulting to zero would silently understate revenue in Gold aggregations.

**VAT extracted before net is derived.** `vat = gross - (gross / VAT_DIVISOR)`, then `net = gross - vat`. This is the HMRC-compliant method and avoids the floating point rounding drift that occurs when converting independently at multiple points.

**`VAT_RATE` defined once at notebook top.** All financial calculations derive from this single value. Changing VAT rate requires one edit and affects every calculation automatically.

**`paid_duration_minutes` uses gross tariff band, not discounted.** A customer who received a PROMO-20 discount on the 1-hour band still bought 60 minutes of entitlement. The duration reflects what was granted, not what was paid.

**`transaction_date` in Gold derived from `transaction_time`.** `entry_timestamp` is null for barrierless sites. Using `transaction_time` ensures every row contributes to daily aggregations regardless of site type.

**`unionByName` rather than `union` for the Silver merge.** Column order differences between providers cannot silently corrupt data when joining by name rather than position.

**Silver writes both individual and unified tables.** Individual provider tables exist for debugging. If a Gold aggregate looks wrong you can isolate which provider contributed the problem in one query.

**Site map as reference table, not embedded logic.** Adding a new provider requires only a new column in `site_map.csv` and a new join condition in Silver. Bronze and Gold need no changes.

---

## Author

Chris O'Malley
[LinkedIn](https://www.linkedin.com/in/chris-o-malley-bb351226/) | chris@tecbitz.co.uk | [GitHub](https://github.com/kronos79-com)
