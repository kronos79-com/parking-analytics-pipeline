import csv
import random
from datetime import datetime, timedelta

random.seed(42)

# ─── Site definitions ─────────────────────────────────────────────────────────
#
# site_type:
#   barrier_anpr    : entry/exit via ANPR cameras, VRM captured 90-95%
#   barrier_ticket  : entry/exit via physical ticket, ticket_number on transaction
#   barrierless     : no entry/exit data, transaction_time only, prepay

PARKTECH_SITES = {
    "PT-1001": {"name": "Castle Street Multi-Storey",  "base_weekday": 350, "profile": "commuter", "site_type": "barrier_anpr",   "payment_type": "pay_on_exit"},
    "PT-1003": {"name": "Central Station NCP",          "base_weekday": 420, "profile": "commuter", "site_type": "barrier_anpr",   "payment_type": "pay_on_exit"},
    "PT-1005": {"name": "Airport Express Park",         "base_weekday": 260, "profile": "airport",  "site_type": "barrier_anpr",   "payment_type": "pay_on_exit"},
}

VENDPARK_SITES = {
    "riverside-retail":  {"name": "Riverside Retail Park", "base_weekday": 190, "profile": "retail",   "site_type": "barrier_anpr",   "payment_type": "pay_on_exit"},
    "westgate-shopping": {"name": "Westgate Shopping",     "base_weekday": 220, "profile": "retail",   "site_type": "barrier_anpr",   "payment_type": "pay_on_exit"},
    "victoria-quarter":  {"name": "Victoria Quarter",      "base_weekday": 130, "profile": "leisure",  "site_type": "barrier_anpr",   "payment_type": "pay_on_exit"},
}

EASYENTRY_SITES = {
    "3a1b2c3d-4e5f-6789-abcd-ef0123456701": {"name": "Seafront Long Stay",   "base_weekday": 110, "profile": "leisure", "site_type": "barrier_ticket", "payment_type": "pay_on_exit", "ticket_prefix": "SLS"},
    "3a1b2c3d-4e5f-6789-abcd-ef0123456702": {"name": "The Lanes Short Stay", "base_weekday": 80,  "profile": "leisure", "site_type": "barrierless",    "payment_type": "prepay",      "open_hour": 8,  "close_hour": 18},
    "3a1b2c3d-4e5f-6789-abcd-ef0123456703": {"name": "Harbour View",         "base_weekday": 65,  "profile": "leisure", "site_type": "barrier_ticket", "payment_type": "pay_on_exit", "ticket_prefix": "HV"},
    "3a1b2c3d-4e5f-6789-abcd-ef0123456704": {"name": "North Street Surface",  "base_weekday": 48,  "profile": "local",   "site_type": "barrierless",    "payment_type": "prepay",      "open_hour": 6,  "close_hour": 20},
}

# ─── Tariff tables (all prices VAT inclusive) ─────────────────────────────────
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

# Midpoint minutes per tariff band for barrierless estimated duration
TARIFF_MIDPOINTS = {
    "3a1b2c3d-4e5f-6789-abcd-ef0123456702": {1.50:45, 2.50:90, 4.00:180, 5.50:300, 7.50:540, 11.00:1080},
    "3a1b2c3d-4e5f-6789-abcd-ef0123456704": {1.00:45, 2.00:90, 3.00:180, 4.00:300, 6.00:540,  8.00:1080},
}

VAT_RATE = 0.20

# ─── Discount definitions ─────────────────────────────────────────────────────
DISCOUNTS = [
    {"code": "BLUE-BADGE",  "product_id": "PROD-BB-001", "type": "free",       "value": 1.00},
    {"code": "SEASON-Q1",   "product_id": "PROD-SQ-002", "type": "free",       "value": 1.00},
    {"code": "PROMO-20",    "product_id": "PROD-PR-003", "type": "percentage", "value": 0.20},
    {"code": "VALID-CODE",  "product_id": "PROD-VC-004", "type": "2hr_free",   "value": None},
]
DISCOUNT_WEIGHTS_NON_BB = [3, 5, 6]

CARD_SCHEME_WEIGHTS = [45, 35, 12, 8]
CARD_SCHEMES        = ["VISA", "MASTERCARD", "AMEX", "MAESTRO"]

CASH_RATE = {
    "commuter": 0.02,
    "airport":  0.01,
    "retail":   0.04,
    "leisure":  0.06,
    "local":    0.15,
}

CARD_PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "CONTACTLESS", "APP"]
CARD_PAYMENT_WEIGHTS = [28, 32, 28, 12]

VRM_LETTERS = "ABCDEFGHJKLMNOPRSTUVWXY"
VRM_DIGITS  = "0123456789"

# ─── Ticket counters ──────────────────────────────────────────────────────────
_ticket_counters = {}

def next_ticket(prefix: str) -> str:
    _ticket_counters[prefix] = _ticket_counters.get(prefix, 0) + 1
    return f"TKT-{prefix}-{_ticket_counters[prefix]:06d}"

# ─── Financial calculations ───────────────────────────────────────────────────

def calc_financials_gross(gross_tariff: float, discount_code: str, discount_type: str,
                           discount_value, site_id: str) -> dict:
    """
    Calculate financial fields for ParkTech and EasyEntry (gross / VAT inclusive reporting).

    gross_tariff    : full tariff price VAT inclusive
    Returns dict with all financial fields on a gross basis.
    """
    gross_tariff = round(gross_tariff, 2)

    # Discount amount (gross, VAT inclusive)
    if discount_type == "free":
        discount_amt = gross_tariff
    elif discount_type == "percentage":
        discount_amt = round(gross_tariff * discount_value, 2)
    elif discount_type == "2hr_free":
        two_hr_gross = get_tariff_charge(site_id, 120)
        discount_amt = min(gross_tariff, round(two_hr_gross, 2))
    else:
        discount_amt = 0.00

    discounted_gross = round(max(0.00, gross_tariff - discount_amt), 2)
    vat_amount       = round(discounted_gross - discounted_gross / (1 + VAT_RATE), 2)
    amount_paid      = discounted_gross

    return {
        "gross_tariff":    gross_tariff,
        "discount_amt":    discount_amt,
        "discounted_gross": discounted_gross,
        "vat_amount":      vat_amount,
        "amount_paid":     amount_paid,
    }

def calc_financials_net(gross_tariff: float, discount_code: str, discount_type: str,
                         discount_value, site_id: str) -> dict:
    """
    Calculate financial fields for VendPark (net / VAT exclusive reporting).

    gross_tariff    : full tariff price VAT inclusive (same tariff table)
    VendPark reports NET so we derive net from gross.
    """
    net_tariff = round(gross_tariff / (1 + VAT_RATE), 2)

    if discount_type == "free":
        discount_amt_net = net_tariff
    elif discount_type == "percentage":
        discount_amt_net = round(net_tariff * discount_value, 2)
    elif discount_type == "2hr_free":
        two_hr_gross     = get_tariff_charge(site_id, 120)
        two_hr_net       = round(two_hr_gross / (1 + VAT_RATE), 2)
        discount_amt_net = min(net_tariff, two_hr_net)
    else:
        discount_amt_net = 0.00

    discounted_net = round(max(0.00, net_tariff - discount_amt_net), 2)
    vat_amount     = round(discounted_net * VAT_RATE, 2)
    amount_paid    = round(discounted_net + vat_amount, 2)   # gross equivalent

    return {
        "net_tariff":      net_tariff,
        "discount_amt_net": discount_amt_net,
        "discounted_net":  discounted_net,
        "vat_amount":      vat_amount,
        "amount_paid":     amount_paid,
    }

# ─── Helper functions ─────────────────────────────────────────────────────────

def get_tariff_charge(site_id: str, duration_minutes: int) -> float:
    for (max_min, price) in TARIFFS[site_id]:
        if duration_minutes <= max_min:
            return price
    return TARIFFS[site_id][-1][1]

def get_volume(base: int, profile: str, dow: int) -> int:
    multipliers = {
        "commuter": [1.0, 1.0, 0.95, 1.0, 0.90, 0.45, 0.30],
        "airport":  [1.0, 0.95, 0.90, 1.0, 1.10, 1.05, 0.85],
        "retail":   [0.70, 0.75, 0.80, 0.85, 1.00, 1.40, 1.20],
        "leisure":  [0.60, 0.65, 0.70, 0.75, 0.90, 1.40, 1.30],
        "local":    [0.80, 0.80, 0.80, 0.80, 0.85, 0.90, 0.60],
    }
    raw    = int(base * multipliers[profile][dow])
    jitter = random.randint(-int(raw * 0.08), int(raw * 0.08))
    return max(1, raw + jitter)

def random_entry_time(date: datetime, profile: str,
                      open_hour: int = None, close_hour: int = None) -> datetime:
    if open_hour is not None and close_hour is not None:
        open_min  = open_hour * 60
        close_min = (close_hour - 1) * 60
        mins      = random.randint(open_min, close_min)
        return date.replace(hour=mins // 60, minute=mins % 60, second=random.randint(0,59))
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
            [random.randint(30,90), random.randint(90,240),
             random.randint(240,600), random.randint(600,1440)],
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

def random_barrierless_duration(site_id: str) -> int:
    tariff  = TARIFFS[site_id]
    bands   = [t[0] for t in tariff]
    weights = [35,30,20,10,4,1] if "ef0123456704" in site_id else [20,25,25,15,10,5]
    band_max = random.choices(bands, weights=weights)[0]
    band_idx = bands.index(band_max)
    band_min = 0 if band_idx == 0 else bands[band_idx - 1]
    return random.randint(band_min + 1, band_max)

def transaction_time_dt(entry_dt: datetime, exit_dt: datetime, payment_type: str) -> datetime:
    offset = timedelta(minutes=random.randint(1,5))
    return entry_dt + offset if payment_type == "prepay" else exit_dt - offset

def random_vrm() -> str:
    return (
        random.choice(VRM_LETTERS) + random.choice(VRM_LETTERS) +
        random.choice(VRM_DIGITS)  + random.choice(VRM_DIGITS)  +
        " "                        +
        random.choice(VRM_LETTERS) + random.choice(VRM_LETTERS) + random.choice(VRM_LETTERS)
    )

def pick_card_scheme() -> str:
    return random.choices(CARD_SCHEMES, weights=CARD_SCHEME_WEIGHTS)[0]

def resolve_acquirer_advam_six(card_scheme: str) -> str:
    return "AMEX" if card_scheme == "AMEX" else "Worldpay"

def build_discount_info(profile: str, site_id: str, gross_tariff: float):
    """Returns (discount_code, product_id, discount_type, discount_value)."""
    rate = 0.10 if profile == "retail" else 0.08
    if random.random() < rate:
        d = random.choices(DISCOUNTS[1:], weights=DISCOUNT_WEIGHTS_NON_BB)[0]
        return d["code"], d["product_id"], d["type"], d.get("value", None)
    return "", "", "none", None

MARCH_DATES = [datetime(2025,3,d) for d in range(1,32)]

# ─── PROVIDER A: ParkTech / Stripe ───────────────────────────────────────────
#
# Financial fields (GROSS / VAT inclusive):
#   gross_charge, discount_amount, discounted_charge, vat_rate, vat_amount, amount_paid
#
# Column order: transaction_id, site_id, site_name, vrm,
#   entry_timestamp, exit_timestamp, transaction_time, duration_minutes,
#   payment_method, card_scheme,
#   gross_charge, discount_code, product_id, discount_amount,
#   discounted_charge, vat_rate, vat_amount, amount_paid,
#   psp, acquirer, psp_reference

def stripe_ref() -> str:
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "ch_" + "".join(random.choices(chars, k=24))

def generate_parktech(path: str):
    rows = []
    anpr_rate = random.uniform(0.90, 0.95)

    for site_id, site in PARKTECH_SITES.items():
        for date in MARCH_DATES:
            for _ in range(get_volume(site["base_weekday"], site["profile"], date.weekday())):
                entry_dt = random_entry_time(date, site["profile"])
                duration = random_duration(site["profile"])
                exit_dt  = entry_dt + timedelta(minutes=duration)
                gross    = get_tariff_charge(site_id, duration)
                is_cash  = random.random() < CASH_RATE[site["profile"]]
                txn_dt   = transaction_time_dt(entry_dt, exit_dt, site["payment_type"])
                vrm      = random_vrm() if random.random() < anpr_rate else ""

                disc_code, prod_id, disc_type, disc_value = build_discount_info(
                    site["profile"], site_id, gross)

                fin = calc_financials_gross(gross, disc_code, disc_type, disc_value, site_id)

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
                    acquirer       = "Stripe"
                    psp_reference  = stripe_ref()

                txn_id = stripe_ref()
                if random.random() < 0.01 and rows:
                    txn_id = random.choice(rows[-50:])["transaction_id"]

                # Dirty: ~2% null gross_charge, ~3% UNKNOWN payment method
                gross_display = "" if (not is_cash and random.random() < 0.02) else str(fin["gross_tariff"])
                if not is_cash and random.random() < 0.03:
                    payment_method = "UNKNOWN"

                rows.append({
                    "transaction_id":    txn_id,
                    "site_id":           site_id,
                    "site_name":         site["name"],
                    "vrm":               vrm,
                    "entry_timestamp":   entry_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "exit_timestamp":    exit_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "transaction_time":  txn_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "duration_minutes":  duration,
                    "payment_method":    payment_method,
                    "card_scheme":       card_scheme,
                    "gross_charge":      gross_display,
                    "discount_code":     disc_code,
                    "product_id":        prod_id,
                    "discount_amount":   str(fin["discount_amt"]),
                    "discounted_charge": str(fin["discounted_gross"]),
                    "vat_rate":          str(VAT_RATE),
                    "vat_amount":        str(fin["vat_amount"]),
                    "amount_paid":       str(fin["amount_paid"]),
                    "psp":               psp,
                    "acquirer":          acquirer,
                    "psp_reference":     psp_reference,
                })

    _write(path, rows)
    print(f"ParkTech : {len(rows):,} rows -> {path}")

# ─── PROVIDER B: VendPark / ADVAM ────────────────────────────────────────────
#
# Financial fields (NET / VAT exclusive):
#   net_charge, discounted_net, vat_rate, vat_amount, amount_paid
#   NOTE: no explicit discount_amount column - implied by net_charge vs discounted_net
#
# Column order (different from ParkTech):
#   car_park_slug, car_park_name, txn_ref, vrm,
#   arrival, departure, transaction_time, stay_minutes,
#   payment_type, card_scheme,
#   net_charge, discount_code, product_id,
#   discounted_net, vat_rate, vat_amount, amount_paid,
#   psp, acquirer, psp_reference

def advam_ref() -> str:
    return "ADVAM-" + datetime(2025,3,random.randint(1,31)).strftime("%Y%m%d") + f"-{random.randint(1,99999999):08d}"

def generate_vendpark(path: str):
    rows = []
    anpr_rate = random.uniform(0.90, 0.95)

    for site_id, site in VENDPARK_SITES.items():
        for date in MARCH_DATES:
            for _ in range(get_volume(site["base_weekday"], site["profile"], date.weekday())):
                entry_dt = random_entry_time(date, site["profile"])
                duration = random_duration(site["profile"])
                exit_dt  = entry_dt + timedelta(minutes=duration)
                gross    = get_tariff_charge(site_id, duration)
                is_cash  = random.random() < CASH_RATE[site["profile"]]
                txn_dt   = transaction_time_dt(entry_dt, exit_dt, site["payment_type"])
                vrm      = random_vrm() if random.random() < anpr_rate else ""

                disc_code, prod_id, disc_type, disc_value = build_discount_info(
                    site["profile"], site_id, gross)

                # VendPark reports NET
                fin = calc_financials_net(gross, disc_code, disc_type, disc_value, site_id)

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

                # Mixed case problem on card payment methods
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
                txn_str   = txn_dt.strftime("%d/%m/%Y %H:%M")

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
                    "car_park_slug":   site_id,
                    "car_park_name":   site["name"],
                    "txn_ref":         f"VP-{date.strftime('%Y%m%d')}-{random.randint(1,99999999):08d}",
                    "vrm":             vrm,
                    "arrival":         entry_str,
                    "departure":       exit_str,
                    "transaction_time": txn_str,
                    "stay_minutes":    duration_disp,
                    "payment_type":    payment_method,
                    "card_scheme":     card_scheme,
                    "net_charge":      str(fin["net_tariff"]),
                    "discount_code":   disc_code,
                    "product_id":      prod_id,
                    "discounted_net":  str(fin["discounted_net"]),
                    "vat_rate":        str(VAT_RATE),
                    "vat_amount":      str(fin["vat_amount"]),
                    "amount_paid":     str(fin["amount_paid"]),
                    "psp":             psp,
                    "acquirer":        acquirer,
                    "psp_reference":   psp_reference,
                })

    _write(path, rows)
    print(f"VendPark : {len(rows):,} rows -> {path}")

# ─── PROVIDER C: EasyEntry / SIX ─────────────────────────────────────────────
#
# Financial fields (GROSS / VAT inclusive, £ symbol on price fields):
#   full_price, discounted_price, vat_pct, tax_amount, charged
#   NOTE: no explicit discount_amount - implied by full_price vs discounted_price
#
# Column order (different again):
#   site_uuid, site_name, id, ticket_number,
#   entry_time, exit_time, transaction_time, duration_mins,
#   payment_method, card_scheme,
#   full_price, discount_code, product_id,
#   discounted_price, vat_pct, tax_amount, charged,
#   psp, acquirer, psp_reference

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
        site_type  = site["site_type"]
        open_hour  = site.get("open_hour",     None)
        close_hour = site.get("close_hour",    None)
        ticket_pfx = site.get("ticket_prefix", None)

        for date in MARCH_DATES:
            for _ in range(get_volume(site["base_weekday"], site["profile"], date.weekday())):
                is_cash = random.random() < CASH_RATE[site["profile"]]

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

                # ── Barrier + Ticket (Seafront Long Stay, Harbour View) ──────
                if site_type == "barrier_ticket":
                    entry_dt  = random_entry_time(date, site["profile"])
                    duration  = random_duration(site["profile"])
                    exit_dt   = entry_dt + timedelta(minutes=duration)
                    gross     = get_tariff_charge(site_id, duration)
                    txn_dt    = transaction_time_dt(entry_dt, exit_dt, site["payment_type"])
                    ticket_no = next_ticket(ticket_pfx)

                    disc_code, prod_id, disc_type, disc_value = build_discount_info(
                        site["profile"], site_id, gross)
                    fin = calc_financials_gross(gross, disc_code, disc_type, disc_value, site_id)

                    # ~4% missing exit, ~2% negative duration (dirty data)
                    if not is_cash and random.random() < 0.04:
                        exit_str      = ""
                        duration_disp = ""
                    else:
                        exit_str      = exit_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                        duration_disp = str(duration)

                    if random.random() < 0.02:
                        duration_disp = str(-random.randint(1,60))

                    rows.append({
                        "site_uuid":         site_id,
                        "site_name":         site["name"],
                        "id":                ee_id(),
                        "ticket_number":     ticket_no,
                        "entry_time":        entry_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "exit_time":         exit_str,
                        "transaction_time":  txn_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "duration_mins":     duration_disp,
                        "payment_method":    payment_method,
                        "card_scheme":       card_scheme,
                        "full_price":        f"£{fin['gross_tariff']:.2f}",
                        "discount_code":     disc_code,
                        "product_id":        prod_id,
                        "discounted_price":  f"£{fin['discounted_gross']:.2f}",
                        "vat_pct":           str(int(VAT_RATE * 100)),
                        "tax_amount":        f"£{fin['vat_amount']:.2f}",
                        "charged":           f"£{fin['amount_paid']:.2f}",
                        "psp":               psp,
                        "acquirer":          acquirer,
                        "psp_reference":     psp_reference,
                    })

                # ── Barrierless prepay (The Lanes, North Street) ─────────────
                elif site_type == "barrierless":
                    txn_dt   = random_entry_time(date, site["profile"], open_hour, close_hour)
                    duration = random_barrierless_duration(site_id)
                    gross    = get_tariff_charge(site_id, duration)

                    disc_code, prod_id, disc_type, disc_value = build_discount_info(
                        site["profile"], site_id, gross)
                    fin = calc_financials_gross(gross, disc_code, disc_type, disc_value, site_id)

                    # Estimated duration midpoint from tariff band
                    midpoints = TARIFF_MIDPOINTS[site_id]
                    est_dur   = midpoints.get(fin["gross_tariff"], 90)

                    rows.append({
                        "site_uuid":         site_id,
                        "site_name":         site["name"],
                        "id":                ee_id(),
                        "ticket_number":     "",
                        "entry_time":        "",
                        "exit_time":         "",
                        "transaction_time":  txn_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "duration_mins":     "",
                        "payment_method":    payment_method,
                        "card_scheme":       card_scheme,
                        "full_price":        f"£{fin['gross_tariff']:.2f}",
                        "discount_code":     disc_code,
                        "product_id":        prod_id,
                        "discounted_price":  f"£{fin['discounted_gross']:.2f}",
                        "vat_pct":           str(int(VAT_RATE * 100)),
                        "tax_amount":        f"£{fin['vat_amount']:.2f}",
                        "charged":           f"£{fin['amount_paid']:.2f}",
                        "psp":               psp,
                        "acquirer":          acquirer,
                        "psp_reference":     psp_reference,
                    })

    _write(path, rows)
    print(f"EasyEntry: {len(rows):,} rows -> {path}")

# ─── Site mapping table ───────────────────────────────────────────────────────

def generate_site_map(path: str):
    rows = [
        {"canonical_site_id":"SITE-001","site_name":"Castle Street Multi-Storey","city":"Brighton","capacity":480,"site_type":"barrier_anpr",   "payment_type":"pay_on_exit","open_hour":"","close_hour":"","ticket_prefix":"","parktech_id":"PT-1001","vendpark_slug":"",               "easyentry_uuid":""},
        {"canonical_site_id":"SITE-002","site_name":"Central Station NCP",        "city":"Brighton","capacity":620,"site_type":"barrier_anpr",   "payment_type":"pay_on_exit","open_hour":"","close_hour":"","ticket_prefix":"","parktech_id":"PT-1003","vendpark_slug":"",               "easyentry_uuid":""},
        {"canonical_site_id":"SITE-003","site_name":"Airport Express Park",       "city":"Gatwick", "capacity":900,"site_type":"barrier_anpr",   "payment_type":"pay_on_exit","open_hour":"","close_hour":"","ticket_prefix":"","parktech_id":"PT-1005","vendpark_slug":"",               "easyentry_uuid":""},
        {"canonical_site_id":"SITE-004","site_name":"Riverside Retail Park",      "city":"Brighton","capacity":320,"site_type":"barrier_anpr",   "payment_type":"pay_on_exit","open_hour":"","close_hour":"","ticket_prefix":"","parktech_id":"",       "vendpark_slug":"riverside-retail",  "easyentry_uuid":""},
        {"canonical_site_id":"SITE-005","site_name":"Westgate Shopping",          "city":"Brighton","capacity":410,"site_type":"barrier_anpr",   "payment_type":"pay_on_exit","open_hour":"","close_hour":"","ticket_prefix":"","parktech_id":"",       "vendpark_slug":"westgate-shopping", "easyentry_uuid":""},
        {"canonical_site_id":"SITE-006","site_name":"Victoria Quarter",           "city":"Brighton","capacity":240,"site_type":"barrier_anpr",   "payment_type":"pay_on_exit","open_hour":"","close_hour":"","ticket_prefix":"","parktech_id":"",       "vendpark_slug":"victoria-quarter",  "easyentry_uuid":""},
        {"canonical_site_id":"SITE-007","site_name":"Seafront Long Stay",         "city":"Brighton","capacity":200,"site_type":"barrier_ticket", "payment_type":"pay_on_exit","open_hour":"","close_hour":"","ticket_prefix":"SLS","parktech_id":"",    "vendpark_slug":"",               "easyentry_uuid":"3a1b2c3d-4e5f-6789-abcd-ef0123456701"},
        {"canonical_site_id":"SITE-008","site_name":"The Lanes Short Stay",       "city":"Brighton","capacity":160,"site_type":"barrierless",    "payment_type":"prepay",     "open_hour":8,  "close_hour":18,"ticket_prefix":"","parktech_id":"",     "vendpark_slug":"",               "easyentry_uuid":"3a1b2c3d-4e5f-6789-abcd-ef0123456702"},
        {"canonical_site_id":"SITE-009","site_name":"Harbour View",               "city":"Brighton","capacity":120,"site_type":"barrier_ticket", "payment_type":"pay_on_exit","open_hour":"","close_hour":"","ticket_prefix":"HV","parktech_id":"",     "vendpark_slug":"",               "easyentry_uuid":"3a1b2c3d-4e5f-6789-abcd-ef0123456703"},
        {"canonical_site_id":"SITE-010","site_name":"North Street Surface",       "city":"Brighton","capacity":80, "site_type":"barrierless",    "payment_type":"prepay",     "open_hour":6,  "close_hour":20,"ticket_prefix":"","parktech_id":"",     "vendpark_slug":"",               "easyentry_uuid":"3a1b2c3d-4e5f-6789-abcd-ef0123456704"},
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
