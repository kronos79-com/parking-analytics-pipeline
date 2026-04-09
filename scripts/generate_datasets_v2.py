import csv
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

# ─── Site definitions ─────────────────────────────────────────────────────────

PARKTECH_SITES = {
    "PT-1001": {"name": "Castle Street Multi-Storey",  "base_weekday": 350, "profile": "commuter"},
    "PT-1003": {"name": "Central Station NCP",          "base_weekday": 420, "profile": "commuter"},
    "PT-1005": {"name": "Airport Express Park",         "base_weekday": 260, "profile": "airport"},
}

VENDPARK_SITES = {
    "riverside-retail":  {"name": "Riverside Retail Park", "base_weekday": 190, "profile": "retail"},
    "westgate-shopping": {"name": "Westgate Shopping",     "base_weekday": 220, "profile": "retail"},
    "victoria-quarter":  {"name": "Victoria Quarter",      "base_weekday": 130, "profile": "leisure"},
}

EASYENTRY_SITES = {
    "3a1b2c3d-4e5f-6789-abcd-ef0123456701": {"name": "Seafront Long Stay",   "base_weekday": 110, "profile": "leisure"},
    "3a1b2c3d-4e5f-6789-abcd-ef0123456702": {"name": "The Lanes Short Stay", "base_weekday": 80,  "profile": "leisure"},
    "3a1b2c3d-4e5f-6789-abcd-ef0123456703": {"name": "Harbour View",         "base_weekday": 65,  "profile": "leisure"},
    "3a1b2c3d-4e5f-6789-abcd-ef0123456704": {"name": "North Street Surface",  "base_weekday": 48,  "profile": "local"},
}

# ─── Tariff tables ────────────────────────────────────────────────────────────
# Bands: (max_minutes, price)
TARIFFS = {
    "PT-1001": [(60,3.50),(120,5.50),(240,8.00),(360,11.00),(720,15.00),(1440,22.00)],
    "PT-1003": [(60,4.00),(120,6.50),(240,9.50),(360,13.00),(720,18.00),(1440,26.00)],
    "PT-1005": [(60,5.00),(120,8.00),(240,12.00),(360,16.00),(720,22.00),(1440,30.00)],
    "riverside-retail":  [(60,2.50),(120,4.00),(240,6.00),(360,8.50),(720,12.00),(1440,18.00)],
    "westgate-shopping": [(60,2.50),(120,4.00),(240,6.00),(360,8.50),(720,12.00),(1440,18.00)],
    "victoria-quarter":  [(60,2.00),(120,3.50),(240,5.50),(360,7.50),(720,10.00),(1440,15.00)],
    "3a1b2c3d-4e5f-6789-abcd-ef0123456701": [(60,2.00),(120,3.50),(240,5.00),(360,7.00),(720,9.00),(1440,12.00)],
    "3a1b2c3d-4e5f-6789-abcd-ef0123456702": [(60,1.50),(120,2.50),(240,4.00),(360,5.50),(720,7.50),(1440,11.00)],
    "3a1b2c3d-4e5f-6789-abcd-ef0123456703": [(60,1.50),(120,2.50),(240,4.00),(360,5.50),(720,7.50),(1440,11.00)],
    "3a1b2c3d-4e5f-6789-abcd-ef0123456704": [(60,1.00),(120,2.00),(240,3.00),(360,4.00),(720,6.00),(1440,8.00)],
}

# ─── Discount definitions ─────────────────────────────────────────────────────
DISCOUNTS = [
    {"code": "BLUE-BADGE",  "product_id": "PROD-BB-001", "type": "free",       "value": 1.00},
    {"code": "SEASON-Q1",   "product_id": "PROD-SQ-002", "type": "free",       "value": 1.00},
    {"code": "PROMO-20",    "product_id": "PROD-PR-003", "type": "percentage", "value": 0.20},
    {"code": "VALID-CODE",  "product_id": "PROD-VC-004", "type": "2hr_free",   "value": None},
]
DISCOUNT_WEIGHTS_NON_BB = [3, 5, 6]  # SEASON-Q1, PROMO-20, VALID-CODE

# ─── Card schemes and cash rates by profile ──────────────────────────────────
# card_scheme weights: VISA, MASTERCARD, AMEX, MAESTRO
CARD_SCHEME_WEIGHTS = [45, 35, 12, 8]
CARD_SCHEMES = ["VISA", "MASTERCARD", "AMEX", "MAESTRO"]

CASH_RATE = {
    "commuter": 0.02,
    "airport":  0.01,
    "retail":   0.04,
    "leisure":  0.06,
    "local":    0.15,
}

# ─── Payment methods (non-cash) ───────────────────────────────────────────────
CARD_PAYMENT_METHODS   = ["CREDIT_CARD", "DEBIT_CARD", "CONTACTLESS", "APP"]
CARD_PAYMENT_WEIGHTS   = [28, 32, 28, 12]

# ─── Helper functions ─────────────────────────────────────────────────────────

def get_tariff_charge(site_id: str, duration_minutes: int) -> float:
    for (max_min, price) in TARIFFS[site_id]:
        if duration_minutes <= max_min:
            return price
    return TARIFFS[site_id][-1][1]

def apply_discount(discount: dict, base_charge: float, site_id: str) -> float:
    if discount["type"] == "free":
        return 0.00
    elif discount["type"] == "percentage":
        return round(base_charge * (1 - discount["value"]), 2)
    elif discount["type"] == "2hr_free":
        free_amount = get_tariff_charge(site_id, 120)
        return max(0.00, round(base_charge - free_amount, 2))
    return base_charge

def get_volume(base: int, profile: str, dow: int) -> int:
    multipliers = {
        "commuter": [1.0, 1.0, 0.95, 1.0, 0.90, 0.45, 0.30],
        "airport":  [1.0, 0.95, 0.90, 1.0, 1.10, 1.05, 0.85],
        "retail":   [0.70, 0.75, 0.80, 0.85, 1.00, 1.40, 1.20],
        "leisure":  [0.60, 0.65, 0.70, 0.75, 0.90, 1.40, 1.30],
        "local":    [0.80, 0.80, 0.80, 0.80, 0.85, 0.90, 0.60],
    }
    raw = int(base * multipliers[profile][dow])
    jitter = random.randint(-int(raw * 0.08), int(raw * 0.08))
    return max(1, raw + jitter)

def random_entry_time(date: datetime, profile: str) -> datetime:
    hour_weights = {
        "commuter": [0,0,0,0,0,1,3,8,10,6,4,3,3,3,4,5,6,7,5,3,2,1,1,0],
        "airport":  [1,1,1,1,2,3,4,5,6,6,5,5,5,5,5,5,5,5,4,4,3,2,2,1],
        "retail":   [0,0,0,0,0,0,0,0,1,3,6,8,8,7,7,7,6,5,4,2,1,0,0,0],
        "leisure":  [0,0,0,0,0,0,0,0,1,3,6,8,8,7,7,7,6,5,4,2,1,0,0,0],
        "local":    [0,0,0,0,0,0,1,2,4,5,5,5,4,4,4,4,3,3,2,1,0,0,0,0],
    }
    hour = random.choices(range(24), weights=hour_weights[profile])[0]
    return date.replace(hour=hour, minute=random.randint(0,59), second=random.randint(0,59))

def random_duration(profile: str) -> int:
    if profile == "commuter":
        return random.choices(
            [random.randint(30,90), random.randint(90,240), random.randint(240,600), random.randint(600,1440)],
            weights=[20,30,40,10])[0]
    elif profile == "airport":
        return random.choices(
            [random.randint(60,240), random.randint(240,1440), random.randint(1440,4320)],
            weights=[20,40,40])[0]
    elif profile in ("retail","leisure"):
        return random.choices(
            [random.randint(15,60), random.randint(60,180), random.randint(180,360)],
            weights=[30,50,20])[0]
    else:
        return random.choices(
            [random.randint(15,60), random.randint(60,240)],
            weights=[60,40])[0]

def pick_card_scheme() -> str:
    return random.choices(CARD_SCHEMES, weights=CARD_SCHEME_WEIGHTS)[0]

def resolve_acquirer_advam_six(card_scheme: str) -> str:
    """ADVAM and SIX: AMEX cards route to AMEX direct, all others to Worldpay."""
    return "AMEX" if card_scheme == "AMEX" else "Worldpay"

def build_discount(vehicle: str, profile: str, site_id: str, base_charge: float):
    """Returns (discount_code, product_id, final_charge)."""
    if vehicle == "DISABLED_BADGE":
        d = DISCOUNTS[0]
        return d["code"], d["product_id"], 0.00

    # Retail sites get slightly higher discount rate due to VALID-CODE
    rate = 0.10 if profile in ("retail",) else 0.08
    if random.random() < rate:
        d = random.choices(DISCOUNTS[1:], weights=DISCOUNT_WEIGHTS_NON_BB)[0]
        return d["code"], d["product_id"], apply_discount(d, base_charge, site_id)

    return "", "", base_charge

VEHICLE_TYPES = ["CAR","CAR","CAR","CAR","MOTORCYCLE","DISABLED_BADGE","VAN"]
MARCH_DATES   = [datetime(2025,3,d) for d in range(1,32)]

# ─── PROVIDER A: ParkTech / Stripe ───────────────────────────────────────────

def stripe_ref() -> str:
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "ch_" + "".join(random.choices(chars, k=24))

def generate_parktech(path: str):
    rows = []
    for site_id, site in PARKTECH_SITES.items():
        for date in MARCH_DATES:
            for _ in range(get_volume(site["base_weekday"], site["profile"], date.weekday())):
                entry_dt  = random_entry_time(date, site["profile"])
                duration  = random_duration(site["profile"])
                exit_dt   = entry_dt + timedelta(minutes=duration)
                base      = get_tariff_charge(site_id, duration)
                vehicle   = random.choice(VEHICLE_TYPES)
                is_cash   = random.random() < CASH_RATE[site["profile"]]

                disc_code, prod_id, final = build_discount(vehicle, site["profile"], site_id, base)

                if is_cash:
                    card_scheme    = "N/A"
                    payment_method = "CASH"
                    psp            = "N/A"
                    acquirer       = "N/A"
                    psp_reference  = "N/A"
                else:
                    card_scheme    = pick_card_scheme()
                    payment_method = random.choices(CARD_PAYMENT_METHODS, weights=CARD_PAYMENT_WEIGHTS)[0]
                    psp            = "Stripe"
                    acquirer       = "Stripe"   # Stripe acquires all card types
                    psp_reference  = stripe_ref()

                txn_id = stripe_ref()
                # ~1% duplicate txn ids
                if random.random() < 0.01 and rows:
                    txn_id = random.choice(rows[-50:])["transaction_id"]
                # ~2% null charge
                charge_display = "" if (not is_cash and random.random() < 0.02) else str(final)
                # ~3% UNKNOWN payment method (card only)
                if not is_cash and random.random() < 0.03:
                    payment_method = "UNKNOWN"

                rows.append({
                    "transaction_id":  txn_id,
                    "site_id":         site_id,
                    "site_name":       site["name"],
                    "entry_timestamp": entry_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "exit_timestamp":  exit_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "duration_minutes": duration,
                    "vehicle_type":    vehicle,
                    "payment_method":  payment_method,
                    "card_scheme":     card_scheme,
                    "charge_amount":   charge_display,
                    "discount_code":   disc_code,
                    "product_id":      prod_id,
                    "psp":             psp,
                    "acquirer":        acquirer,
                    "psp_reference":   psp_reference,
                })

    _write(path, rows)
    print(f"ParkTech : {len(rows):,} rows -> {path}")

# ─── PROVIDER B: VendPark / ADVAM ────────────────────────────────────────────

def advam_ref() -> str:
    return "ADVAM-" + datetime(2025,3,random.randint(1,31)).strftime("%Y%m%d") + f"-{random.randint(1,99999999):08d}"

def generate_vendpark(path: str):
    rows = []
    for site_id, site in VENDPARK_SITES.items():
        for date in MARCH_DATES:
            for _ in range(get_volume(site["base_weekday"], site["profile"], date.weekday())):
                entry_dt = random_entry_time(date, site["profile"])
                duration = random_duration(site["profile"])
                exit_dt  = entry_dt + timedelta(minutes=duration)
                base     = get_tariff_charge(site_id, duration)
                vehicle  = random.choice(VEHICLE_TYPES)
                is_cash  = random.random() < CASH_RATE[site["profile"]]

                disc_code, prod_id, final = build_discount(vehicle, site["profile"], site_id, base)

                if is_cash:
                    card_scheme    = "N/A"
                    raw_method     = "CASH"
                    psp            = "N/A"
                    acquirer       = "N/A"
                    psp_reference  = "N/A"
                else:
                    card_scheme    = pick_card_scheme()
                    raw_method     = random.choices(CARD_PAYMENT_METHODS, weights=CARD_PAYMENT_WEIGHTS)[0]
                    psp            = "ADVAM"
                    acquirer       = resolve_acquirer_advam_six(card_scheme)
                    psp_reference  = advam_ref()

                # Mixed case problem (card payments only)
                if not is_cash:
                    variant = random.choices(["upper","lower","title"], weights=[40,30,30])[0]
                    if variant == "lower":
                        payment_method = raw_method.lower()
                    elif variant == "title":
                        payment_method = raw_method.replace("_"," ").title().replace(" ","_")
                    else:
                        payment_method = raw_method
                else:
                    payment_method = raw_method

                entry_str = entry_dt.strftime("%d/%m/%Y %H:%M")

                # ~3% exit before entry
                if random.random() < 0.03:
                    exit_str      = (entry_dt - timedelta(minutes=random.randint(5,30))).strftime("%d/%m/%Y %H:%M")
                    duration_disp = ""
                else:
                    exit_str      = exit_dt.strftime("%d/%m/%Y %H:%M")
                    duration_disp = str(duration)

                # ~2% null duration
                if random.random() < 0.02:
                    duration_disp = ""

                rows.append({
                    "txn_ref":       f"WP-{date.strftime('%Y%m%d')}-{random.randint(1,99999999):08d}",
                    "car_park_slug": site_id,
                    "car_park_name": site["name"],
                    "arrival":       entry_str,
                    "departure":     exit_str,
                    "stay_minutes":  duration_disp,
                    "vehicle":       vehicle,
                    "payment_type":  payment_method,
                    "card_scheme":   card_scheme,
                    "amount_charged": str(final),
                    "discount_code": disc_code,
                    "product_id":    prod_id,
                    "psp":           psp,
                    "acquirer":      acquirer,
                    "psp_reference": psp_reference,
                })

    _write(path, rows)
    print(f"VendPark : {len(rows):,} rows -> {path}")

# ─── PROVIDER C: EasyEntry / SIX ─────────────────────────────────────────────

ee_counter = 1

def ee_id() -> str:
    global ee_counter
    val = f"EE-{ee_counter:06d}"
    ee_counter += 1
    return val

def six_ref() -> str:
    return "SIX-" + "".join(random.choices("0123456789ABCDEF", k=16))

def generate_easyentry(path: str):
    rows = []
    for site_id, site in EASYENTRY_SITES.items():
        for date in MARCH_DATES:
            for _ in range(get_volume(site["base_weekday"], site["profile"], date.weekday())):
                entry_dt = random_entry_time(date, site["profile"])
                duration = random_duration(site["profile"])
                exit_dt  = entry_dt + timedelta(minutes=duration)
                base     = get_tariff_charge(site_id, duration)
                vehicle  = random.choice(VEHICLE_TYPES)
                is_cash  = random.random() < CASH_RATE[site["profile"]]

                disc_code, prod_id, final = build_discount(vehicle, site["profile"], site_id, base)

                if is_cash:
                    card_scheme    = "N/A"
                    payment_method = "CASH"
                    psp            = "N/A"
                    acquirer       = "N/A"
                    psp_reference  = "N/A"
                else:
                    card_scheme    = pick_card_scheme()
                    payment_method = random.choices(CARD_PAYMENT_METHODS, weights=CARD_PAYMENT_WEIGHTS)[0]
                    psp            = "SIX"
                    acquirer       = resolve_acquirer_advam_six(card_scheme)
                    psp_reference  = six_ref()

                # Dirty: charge as string with £ symbol
                charge_str = f"£{final:.2f}"

                # ~4% missing exit timestamp (not cash)
                if not is_cash and random.random() < 0.04:
                    exit_str      = ""
                    duration_disp = ""
                else:
                    exit_str      = exit_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    duration_disp = str(duration)

                # ~2% negative duration
                if random.random() < 0.02:
                    duration_disp = str(-random.randint(1,60))

                rows.append({
                    "id":            ee_id(),
                    "site_uuid":     site_id,
                    "site_name":     site["name"],
                    "entry_time":    entry_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "exit_time":     exit_str,
                    "duration_mins": duration_disp,
                    "vehicle_class": vehicle,
                    "payment_method": payment_method,
                    "card_scheme":   card_scheme,
                    "charge":        charge_str,
                    "discount_code": disc_code,
                    "product_id":    prod_id,
                    "psp":           psp,
                    "acquirer":      acquirer,
                    "psp_reference": psp_reference,
                })

    _write(path, rows)
    print(f"EasyEntry: {len(rows):,} rows -> {path}")

# ─── Site mapping table ───────────────────────────────────────────────────────

def generate_site_map(path: str):
    rows = [
        {"canonical_site_id":"SITE-001","site_name":"Castle Street Multi-Storey","city":"Brighton","capacity":480,"parktech_id":"PT-1001","vendpark_slug":"","easyentry_uuid":""},
        {"canonical_site_id":"SITE-002","site_name":"Central Station NCP",        "city":"Brighton","capacity":620,"parktech_id":"PT-1003","vendpark_slug":"","easyentry_uuid":""},
        {"canonical_site_id":"SITE-003","site_name":"Airport Express Park",       "city":"Gatwick", "capacity":900,"parktech_id":"PT-1005","vendpark_slug":"","easyentry_uuid":""},
        {"canonical_site_id":"SITE-004","site_name":"Riverside Retail Park",      "city":"Brighton","capacity":320,"parktech_id":"","vendpark_slug":"riverside-retail","easyentry_uuid":""},
        {"canonical_site_id":"SITE-005","site_name":"Westgate Shopping",          "city":"Brighton","capacity":410,"parktech_id":"","vendpark_slug":"westgate-shopping","easyentry_uuid":""},
        {"canonical_site_id":"SITE-006","site_name":"Victoria Quarter",           "city":"Brighton","capacity":240,"parktech_id":"","vendpark_slug":"victoria-quarter","easyentry_uuid":""},
        {"canonical_site_id":"SITE-007","site_name":"Seafront Long Stay",         "city":"Brighton","capacity":200,"parktech_id":"","vendpark_slug":"","easyentry_uuid":"3a1b2c3d-4e5f-6789-abcd-ef0123456701"},
        {"canonical_site_id":"SITE-008","site_name":"The Lanes Short Stay",       "city":"Brighton","capacity":160,"parktech_id":"","vendpark_slug":"","easyentry_uuid":"3a1b2c3d-4e5f-6789-abcd-ef0123456702"},
        {"canonical_site_id":"SITE-009","site_name":"Harbour View",               "city":"Brighton","capacity":120,"parktech_id":"","vendpark_slug":"","easyentry_uuid":"3a1b2c3d-4e5f-6789-abcd-ef0123456703"},
        {"canonical_site_id":"SITE-010","site_name":"North Street Surface",       "city":"Brighton","capacity":80, "parktech_id":"","vendpark_slug":"","easyentry_uuid":"3a1b2c3d-4e5f-6789-abcd-ef0123456704"},
    ]
    _write(path, rows)
    print(f"Site map : {len(rows)} rows -> {path}")

# ─── Utility ─────────────────────────────────────────────────────────────────

def _write(path: str, rows: list):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

# ─── Run ─────────────────────────────────────────────────────────────────────

import os
os.makedirs("data/sources", exist_ok=True)

generate_parktech ("data/sources/parktech_march_2025.csv")
generate_vendpark ("data/sources/vendpark_march_2025.csv")
generate_easyentry("data/sources/easyentry_march_2025.csv")
generate_site_map ("data/sources/site_map.csv")

