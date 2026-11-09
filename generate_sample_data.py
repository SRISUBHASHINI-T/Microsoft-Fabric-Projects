"""
Generate fictional source tables for notebook 01 (store payment reconciliation)
as CSV files. Upload them to a Fabric Lakehouse (Files -> Load to Tables) under
the schema/table names noted below, then run the notebook with SEND_EMAIL=False.

    csv/sale_headers.csv        -> lake_refined.web_banyan.sale_headers
    csv/sale_ledger.csv         -> lake_refined.web_banyan.sale_ledger
    csv/tender_log.csv          -> lake_refined.web_banyan.tender_log
    csv/edc_settlements.csv     -> lake_serve.mart.edc_settlements
    csv/qr_collections.csv      -> lake_serve.mart.qr_collections
    csv/till_cash_<outlet>.csv  -> lake_serve.mart.till_cash_<outlet>

The data deliberately includes multi-day bills (advance + balance), split
tenders, coupon redemptions and reversals, and a few terminal mismatches so
the differences columns light up.

Usage:  python generate_sample_data.py [--days 30] [--seed 7]
"""

import argparse
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

STORES = {
    "Downtown":  {"locations": ["Banyan Downtown", "Banyan Downtown, Metro City"], "terminals": [900101, 900102], "upi": "Banyan - DT"},
    "Harbor":    {"locations": ["Banyan Harbor"],                                   "terminals": [900103],         "upi": "Banyan - HBR"},
    "Hillview":  {"locations": ["Banyan Hillview", "Banyan Hillview, North"],       "terminals": [900104],         "upi": "Banyan - HV"},
    "Lakeside":  {"locations": ["Banyan Lakeside", "Banyan Pop-up Lakeside"],         "terminals": [900105, 900106], "upi": "Banyan - Floor2"},
    "Riverside": {"locations": ["Banyan Riverside"],                                "terminals": [900107],         "upi": "Banyan - RS"},
}
IST = timezone(timedelta(hours=5, minutes=30))


def ist_noonish(d: date) -> datetime:
    """Random store-hours time on day d, returned in UTC (as the e-com feed stores it)."""
    local = datetime(d.year, d.month, d.day, random.randint(10, 20), random.randint(0, 59), tzinfo=IST)
    return local.astimezone(timezone.utc)


def main(days: int, seed: int, out: Path):
    random.seed(seed)
    out.mkdir(parents=True, exist_ok=True)
    today = date.today()
    start = today - timedelta(days=days)

    sale_headers, events, tender_log, terminal, upi = [], [], [], [], []
    cash = {s: [] for s in STORES}
    bill_ref = 100000

    for offset in range(days):
        day = start + timedelta(days=offset)
        for outlet, cfg in STORES.items():
            day_cash = 0.0
            for _ in range(random.randint(4, 12)):
                bill_ref += 1
                amt_inr = round(random.choice([1499, 2999, 4999, 8999, 15999, 24999]) * random.uniform(0.9, 1.1), 2)
                sale_headers.append({"bill_ref": bill_ref, "receipt_no": f"#BN{bill_ref}",
                               "outlet_label": random.choice(cfg["locations"])})

                two_day = random.random() < 0.12 and offset < days - 3
                ts = ist_noonish(day)

                def pay(when, tender_src, amt, entry_kind="capture"):
                    tender_log.append({"bill_ref": bill_ref, "logged_at": when.isoformat(), "tender_src": tender_src,
                                     "entry_kind": entry_kind, "amt_inr": amt, "state_flag": "settled"})
                    local_day = when.astimezone(IST).date()
                    if tender_src in ("card_edc", "upi_qr"):
                        recorded = amt
                        if random.random() < 0.03:          # occasional mismatch
                            recorded = round(amt - random.choice([100, 500, 1000]), 2)
                        if tender_src == "upi_qr" and random.random() < 0.4:
                            upi.append({"qr_outlet": cfg["upi"], "qr_ts": local_day.isoformat(), "amt_inr": recorded})
                        else:
                            terminal.append({"edc_id": random.choice(cfg["terminals"]),
                                             "edc_mode": "CRD" if tender_src == "card_edc" else "QR", "amt_inr": recorded,
                                             "edc_ts": local_day.isoformat(), "edc_state": "Approved"})

                if two_day:
                    # Advance today (sale not yet recognised), balance + sale later
                    advance = round(amt_inr * 0.3, 2)
                    later = ist_noonish(day + timedelta(days=random.randint(2, 3)))
                    events.append({"bill_ref": bill_ref, "posted_at": ts.isoformat(), "amt_inr": 0.0})
                    events.append({"bill_ref": bill_ref, "posted_at": later.isoformat(), "amt_inr": amt_inr})
                    pay(ts, "upi_qr", advance)
                    pay(later, "card_edc", round(amt_inr - advance, 2))
                    continue

                events.append({"bill_ref": bill_ref, "posted_at": ts.isoformat(), "amt_inr": amt_inr})
                r = random.random()
                if r < 0.45:
                    pay(ts, "card_edc", amt_inr)
                elif r < 0.80:
                    pay(ts, "upi_qr", amt_inr)
                elif r < 0.90:
                    split = round(amt_inr * 0.5, 2)
                    pay(ts, "card_edc", split)
                    day_cash += amt_inr - split
                elif r < 0.95:
                    cp = round(min(2000, amt_inr), 2)
                    pay(ts, "coupon", cp)
                    pay(ts, "upi_qr", round(amt_inr - cp, 2))
                    if random.random() < 0.3:
                        # Bill reversed later the same day: coupon reversal is
                        # logged POSITIVE (the notebook negates it).
                        pay(ts, "coupon", cp, entry_kind="reversal")
                else:
                    day_cash += amt_inr

            if day_cash:
                cash[outlet].append({"posting_day": day.isoformat(), "deposit_inr": round(day_cash, 2)})

    frames = {
        "sale_headers": sale_headers, "sale_ledger": events, "tender_log": tender_log,
        "edc_settlements": terminal, "qr_collections": upi,
        **{f"till_cash_{s.lower()}": rows for s, rows in cash.items()},
    }
    for name, rows in frames.items():
        pd.DataFrame(rows).to_csv(out / f"{name}.csv", index=False)
        print(f"{name:<28} {len(rows):>6} rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", type=Path, default=Path(__file__).parent / "csv")
    a = ap.parse_args()
    main(a.days, a.seed, a.out)
