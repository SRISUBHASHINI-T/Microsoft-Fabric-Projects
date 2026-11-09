"""
Weekly Brand Performance Dashboard
==================================
Fabric / PySpark notebook that builds a one-page, brand-wise weekly dashboard
(Excel) and emails it:

    SALES      week sales, MTD sales, MTD target, MTD achievement %
    PURCHASE   GRN for the week and MTD (supplier bills)
    RETURNS    purchase returns for the week and MTD
    PAYMENTS   supplier vs other payments, week and MTD
    STOCK      sell value, cost value, margin %, stock cover (weeks)

Interesting bits of logic kept from the production version:
  * Sales for a brand can come from several systems (store ERP + e-commerce).
  * A sub-brand (the "Lumen" jewellery line) is sold through two brands' online
    stores but reported under ONE brand - so it is carved out by a maker-name regex
    from one brand and added to the other.
  * One brand's legacy POS already contains its online orders, so the
    e-commerce feed is NOT added (it would double count) - printed only.
  * A day/month swap fix for spreadsheet-fed dates that arrive already parsed
    the wrong way round (so `dayfirst=True` is silently ignored).
  * GRN and returns that live in ONE table, split by a direction column.
  * Payment trackers split into "supplier" vs "other" by a category column
    whose name and allowed values differ per entity.

All brand, table, column and e-mail values are fictional placeholders.
"""

# ── CELL 1: Config ─────────────────────────────────────────────────────────
import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")

REPORT_START = pd.Timestamp("2026-10-05")
REPORT_END = pd.Timestamp("2026-10-11")
MTD_START = REPORT_END.replace(day=1)
OUT_PATH = "/lakehouse/default/Files/weekly_brand_dashboard.xlsx"

BRANDS = ["BANYAN", "CEDAR", "MAPLE", "WILLOW"]
SUB_BRAND_PATTERN = r"lumen"            # maker-name regex for the jewellery line
STOCK_COVER_FACTOR = 0.75               # cover = sell value / (week sales * factor)
MAPLE_COST_RATIO = 0.60                 # Maple has no reliable unit cost; estimate

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
EMAIL_TO = [e for e in os.getenv("WEEKLY_EMAIL_TO", "leadership@example.com").split(",") if e]

week_start, week_end, month_start = REPORT_START, REPORT_END, MTD_START
print(f"Week {week_start.date()} -> {week_end.date()} | MTD from {month_start.date()}")


def read(table):
    return spark.read.table(table).toPandas()


# ── CELL 2: Helpers ────────────────────────────────────────────────────────
def to_day(series, dayfirst=False):
    return pd.to_datetime(series, errors="coerce", dayfirst=dayfirst).dt.normalize()


def utc_to_local_day(series, tz="Asia/Kolkata"):
    return (pd.to_datetime(series, utc=True, errors="coerce")
            .dt.tz_convert(tz).dt.tz_localize(None).dt.normalize())


def fix_swapped_date(series):
    """
    Some sheet-fed columns arrive as datetime64 already parsed month-first,
    so 03-Jul becomes 07-Mar and dayfirst=True can't help. Swap day/month
    back. Values whose day > 12 can't be swapped and are left as is.
    """
    def swap(ts):
        if pd.isna(ts):
            return ts
        try:
            return ts.replace(day=ts.month, month=ts.day)
        except ValueError:
            return ts
    return series.apply(swap)


def to_amount(series):
    cleaned = series.astype(str).str.replace(r"[,₹$\s]", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce").fillna(0.0)


def week_mtd(df, date_col, amt_col):
    """Return (week_total, mtd_total)."""
    if df.empty:
        return 0.0, 0.0
    d = pd.DataFrame({"d": df[date_col], "a": to_amount(df[amt_col])}).dropna(subset=["d"])
    week = d.loc[d["d"].between(week_start, week_end), "a"].sum()
    mtd = d.loc[d["d"].between(month_start, week_end), "a"].sum()
    return float(week), float(mtd)


def as_sales(df, date_col, amt_col):
    """Normalise any sales source to (_date, _amt) so sources can be concatenated."""
    return df.rename(columns={date_col: "_date", amt_col: "_amt"})[["_date", "_amt"]]


def payment_split(df, date_col, amt_col, cat_col, status_col, supplier_values):
    """Paid rows only; supplier categories vs everything else (blanks = other)."""
    d = df[df[status_col].astype(str).str.strip().str.lower() == "paid"]
    cat = d[cat_col].fillna("").astype(str).str.strip().str.lower()
    to_vendor = cat.isin([v.lower() for v in supplier_values])
    sw, smtd = week_mtd(d[to_vendor], date_col, amt_col)
    ow, omtd = week_mtd(d[~to_vendor], date_col, amt_col)
    return {"sup_w": sw, "sup_mtd": smtd, "oth_w": ow, "oth_mtd": omtd}


def grn_returns_split(df, date_col, amt_col, direction_col):
    """One ledger holding both: 'receipt' = GRN, 'dispatch'/'credit note' = returns."""
    flow_dir = df[direction_col].fillna("").astype(str).str.strip().str.lower()
    grn = week_mtd(df[flow_dir == "receipt"], date_col, amt_col)
    ret = week_mtd(df[flow_dir.isin(["dispatch", "credit note"])], date_col, amt_col)
    # Return rows are stored negative; report them as positive amounts.
    return grn, (abs(ret[0]), abs(ret[1]))


def debug_dates(label, df, col):
    print(f"[{label}] rows={len(df)} nulls={df[col].isna().sum()} "
          f"min={df[col].min()} max={df[col].max()} "
          f"in_week={df[col].between(week_start, week_end).sum()} "
          f"in_mtd={df[col].between(month_start, week_end).sum()}")


# ── CELL 3: Purchases (GRN) ────────────────────────────────────────────────
bills = {
    "BANYAN": (read("lake_serve.mart.purch_inv_banyan"), "inv_dt", "inv_gross"),
    "CEDAR":  (read("lake_serve.mart.purch_inv_cedar"),  "inv_dt", "inv_net"),
    "MAPLE":  (read("lake_serve.mart.purch_inv_maple"),  "inv_dt", "gross_inr"),
    "LUMEN":  (read("lake_serve.mart.purch_inv_lumen"),  "inv_dt", "gross_inr"),
}
for df, dcol, _ in bills.values():
    df[dcol] = to_day(df[dcol], dayfirst=True)

grn = {b: week_mtd(*bills[b]) for b in ["BANYAN", "CEDAR", "MAPLE", "LUMEN"]}
grn["MAPLE"] = tuple(m + l for m, l in zip(grn["MAPLE"], grn.pop("LUMEN")))   # Lumen reports under Maple

willow_ledger = read("lake_refined.mart.willow_stock_movements")
willow_ledger["posting_day"] = to_day(willow_ledger["posting_day"], dayfirst=True)
grn["WILLOW"], willow_returns = grn_returns_split(
    willow_ledger, "posting_day", "ledger_net", "flow_dir")


# ── CELL 4: Purchase returns ───────────────────────────────────────────────
ret_banyan = read("lake_serve.mart.debit_notes_banyan")
ret_banyan["shipped_on"] = fix_swapped_date(to_day(ret_banyan["shipped_on"], dayfirst=True))
# Only returns already booked against a bill count.
ret_banyan = ret_banyan[ret_banyan["booking_state"].astype(str).str.strip().str.lower() == "booked"]
debug_dates("BANYAN returns", ret_banyan, "shipped_on")

returns_src = {
    "CEDAR": "lake_serve.mart.debit_notes_cedar",
    "MAPLE": "lake_serve.mart.debit_notes_maple",
    "LUMEN": "lake_serve.mart.debit_notes_lumen",
}
ret = {}
for banner, table in returns_src.items():
    df = read(table)
    df["debit_dt"] = to_day(df["debit_dt"], dayfirst=True)
    debug_dates(f"{banner} returns", df, "debit_dt")
    ret[banner] = week_mtd(df, "debit_dt", "gross_inr")

returns = {
    "BANYAN": week_mtd(ret_banyan, "shipped_on", "gross_inr"),
    "CEDAR":  ret["CEDAR"],
    "MAPLE":  tuple(a + b for a, b in zip(ret["MAPLE"], ret["LUMEN"])),
    "WILLOW": willow_returns,
}


# ── CELL 5: Payments ───────────────────────────────────────────────────────
# Each entity's tracker uses its own column names and supplier labels.
PAYMENT_TRACKERS = {
    "BANYAN": ("lake_serve.mart.outflow_banyan", "paid_dt", "amt_inr", "spend_head",    "state_flag", ["Vendor Settlement"], True),
    "CEDAR":  ("lake_serve.mart.outflow_cedar",  "paid_dt", "amt_inr", "spend_head",    "state_flag", ["Vendor Settlement"], True),
    "MAPLE":  ("lake_serve.mart.outflow_maple",  "paid_dt", "amt_inr", "narration_txt", "state_flag", ["Vendor Pay"], True),
    "WILLOW": ("lake_serve.mart.outflow_willow", "settled_dt",      "amt_inr", "cost_head", "state_flag", ["Supplier Remit"], False),
}
tender_log = {}
for banner, (table, dcol, acol, ccol, scol, sup_vals, swapped) in PAYMENT_TRACKERS.items():
    df = read(table)
    df[dcol] = to_day(df[dcol], dayfirst=True)
    if swapped:
        df[dcol] = fix_swapped_date(df[dcol])
    tender_log[banner] = payment_split(df, dcol, acol, ccol, scol, sup_vals)


# ── CELL 6: Sales ──────────────────────────────────────────────────────────
def ecom_with_vendor(schema):
    """E-commerce sale events joined to the SKU maker via line items."""
    events = read(f"{schema}.sale_ledger")
    events["posted_at"] = utc_to_local_day(events["posted_at"])
    vendor_map = read(f"{schema}.sale_line_map")[["line_ref", "sku_ref"]].merge(
        read(f"{schema}.sku_master")[["sku_ref", "maker"]], on="sku_ref", how="left")
    out = events.merge(vendor_map, on="line_ref", how="left")
    out["maker"] = out["maker"].fillna("").astype(str)
    return out


def split_sub_brand(df):
    is_sub = df["maker"].str.contains(SUB_BRAND_PATTERN, case=False, regex=True, na=False)
    return df[~is_sub], df[is_sub]


banyan_erp = spark.sql("""
    SELECT billed_at, collected_inr
    FROM lake_refined.till_erp.bill_headers
    WHERE till_user <> 'sysadmin'
      AND till_label NOT LIKE '%(retired)%'
""").toPandas()
banyan_erp["billed_at"] = to_day(banyan_erp["billed_at"])

banyan_online, lumen_via_banyan = split_sub_brand(ecom_with_vendor("lake_refined.web_banyan"))
maple_online, lumen_via_maple = split_sub_brand(ecom_with_vendor("lake_refined.web_maple"))
cedar_online = ecom_with_vendor("lake_refined.web_cedar")
fern_online = ecom_with_vendor("lake_refined.web_fern")          # sister store, reported under Cedar

willow_pos = spark.sql("""
    SELECT inv_dt, amt_inr
    FROM lake_refined.till_legacy.receipts
    WHERE void_flag <> 'Y'
""").toPandas()
willow_pos["inv_dt"] = to_day(willow_pos["inv_dt"])
willow_online = ecom_with_vendor("lake_refined.web_willow")       # diagnostic only

sales_sources = {
    "BANYAN": [as_sales(banyan_erp, "billed_at", "collected_inr"),
               as_sales(banyan_online, "posted_at", "amt_inr")],
    "CEDAR":  [as_sales(cedar_online, "posted_at", "amt_inr"),
               as_sales(fern_online, "posted_at", "amt_inr")],
    "MAPLE":  [as_sales(maple_online, "posted_at", "amt_inr"),
               as_sales(lumen_via_maple, "posted_at", "amt_inr"),
               as_sales(lumen_via_banyan, "posted_at", "amt_inr")],
    # Legacy POS already includes online orders -> don't add the e-com feed.
    "WILLOW": [as_sales(willow_pos, "inv_dt", "amt_inr")],
}
sales = {b: week_mtd(pd.concat(srcs, ignore_index=True), "_date", "_amt")
         for b, srcs in sales_sources.items()}

print("Lumen carved out of Banyan:", week_mtd(as_sales(lumen_via_banyan, "posted_at", "amt_inr"), "_date", "_amt"))
print("Willow e-com (not added):  ", week_mtd(as_sales(willow_online, "posted_at", "amt_inr"), "_date", "_amt"))
for b in BRANDS:
    print(f"{b:<7} week={sales[b][0]:>14,.0f}  mtd={sales[b][1]:>14,.0f}")


# ── CELL 7: Targets ────────────────────────────────────────────────────────
def target(table):
    df = read(table)
    df["goal_day"] = to_day(df["goal_day"], dayfirst=True)
    return week_mtd(df, "goal_day", "day_goal")


targets = {
    "BANYAN": target("lake_refined.mart.day_goal_banyan"),
    "CEDAR":  target("lake_refined.mart.day_goal_cedar"),
    "MAPLE":  tuple(a + b for a, b in zip(target("lake_refined.mart.day_goal_maple"),
                                          target("lake_refined.mart.day_goal_lumen"))),
    "WILLOW": target("lake_refined.mart.day_goal_willow"),
}


# ── CELL 8: Stock position ─────────────────────────────────────────────────
def scalar(sql, col):
    v = spark.sql(sql).collect()[0][col]
    return float(v or 0)


EXCLUDED_TYPES = "('sample', 'packaging', 'internal', 'voucher')"

stock = {}
stock["BANYAN"] = {
    "sell": scalar("""
        SELECT SUM(q.pickable_qty * p.mrp_inr) AS v
        FROM lake_refined.till_erp.bin_qty q JOIN lake_refined.till_erp.sku_master p ON q.sku_ref = p.id
        WHERE q.pickable_qty > 0
          AND q.bin_label NOT IN ('WEB/WH', 'VIRT/MFG', 'IN-TRANSIT')
    """, "v") + scalar(f"""
        SELECT SUM(v.web_qty * v.sell_inr) AS v
        FROM lake_refined.web_banyan.sku_options v JOIN lake_refined.web_banyan.sku_master p USING (sku_ref)
        WHERE p.state_flag = 'live' AND LOWER(p.maker) NOT RLIKE '{SUB_BRAND_PATTERN}'
          AND p.sku_class NOT IN {EXCLUDED_TYPES}
    """, "v"),
    # Online cost isn't tracked for Banyan; cost = store stock only.
    "cost_val": scalar("""
        SELECT SUM(q.pickable_qty * p.std_cost_inr) AS v
        FROM lake_refined.till_erp.bin_qty q JOIN lake_refined.till_erp.sku_master p ON q.sku_ref = p.id
        WHERE q.pickable_qty > 0
          AND q.bin_label NOT IN ('WEB/WH', 'VIRT/MFG', 'IN-TRANSIT')
    """, "v"),
}

ecom_inventory_sql = """
    SELECT SUM(i.pickable_qty * i.cost_inr) AS cost_val, SUM(i.pickable_qty * i.opt_sell_inr) AS sell
    FROM lake_refined.{schema}.bin_levels i
    JOIN lake_refined.{schema}.sku_options v ON i.opt_ref = v.opt_ref
    JOIN lake_refined.{schema}.sku_master p ON v.sku_ref = p.sku_ref
    WHERE p.state_flag = 'live' AND p.sku_class NOT IN {excluded}
"""
cedar_inv = spark.sql(ecom_inventory_sql.format(schema="web_cedar", excluded=EXCLUDED_TYPES)).collect()[0]
stock["CEDAR"] = {"sell": float(cedar_inv["sell"] or 0), "cost_val": float(cedar_inv["cost_val"] or 0)}

maple_sell = (
    float(spark.sql(ecom_inventory_sql.format(schema="web_maple", excluded=EXCLUDED_TYPES)).collect()[0]["sell"] or 0)
    + scalar(f"""
        SELECT SUM(v.web_qty * v.sell_inr) AS v
        FROM lake_refined.web_banyan.sku_options v JOIN lake_refined.web_banyan.sku_master p USING (sku_ref)
        WHERE p.state_flag = 'live' AND LOWER(p.maker) RLIKE '{SUB_BRAND_PATTERN}'
    """, "v")
)
stock["MAPLE"] = {"sell": maple_sell, "cost_val": maple_sell * MAPLE_COST_RATIO}

willow_inv = spark.sql("""
    SELECT SUM(onhand_qty * cost_rate) AS cost_val, SUM(onhand_qty * sell_rate) AS sell
    FROM lake_refined.till_legacy.shelf_book
""").collect()[0]
stock["WILLOW"] = {"sell": float(willow_inv["sell"] or 0), "cost_val": float(willow_inv["cost_val"] or 0)}


def margin_pct(sell, cost_val):
    return (sell - cost_val) / sell * 100 if sell else 0.0


def stock_cover(sell, week_sales):
    denom = week_sales * STOCK_COVER_FACTOR
    return sell / denom if denom else 0.0


# ── CELL 9: Assemble metric rows ───────────────────────────────────────────
def col(metric_fn):
    """Per-brand values + group total."""
    vals = [metric_fn(b) for b in BRANDS]
    return vals + [sum(vals)]


group_sell = sum(stock[b]["sell"] for b in BRANDS)
group_cost = sum(stock[b]["cost_val"] for b in BRANDS)
group_week_sales = sum(sales[b][0] for b in BRANDS)
group_mtd_sales = sum(sales[b][1] for b in BRANDS)
group_mtd_target = sum(targets[b][1] for b in BRANDS)


def achievement(s, t):
    return s / t * 100 if t else 0.0


LAYOUT = [
    ("SALES", [
        ("Week Sales",        col(lambda b: sales[b][0]),   dict(bold=True)),
        ("MTD Sales",         col(lambda b: sales[b][1]),   {}),
        ("MTD Target",        col(lambda b: targets[b][1]), {}),
        ("MTD Achievement %", [achievement(sales[b][1], targets[b][1]) for b in BRANDS]
                              + [achievement(group_mtd_sales, group_mtd_target)], dict(bold=True, pct=True)),
    ]),
    ("PURCHASE & GRN", [
        ("GRN This Week", col(lambda b: grn[b][0]), {}),
        ("MTD GRN",       col(lambda b: grn[b][1]), {}),
    ]),
    ("RETURNS", [
        ("Returns This Week", col(lambda b: returns[b][0]), {}),
        ("MTD Returns",       col(lambda b: returns[b][1]), {}),
    ]),
    ("PAYMENTS", [
        ("Supplier - Week",       col(lambda b: tender_log[b]["sup_w"]), {}),
        ("Other Expenses - Week", col(lambda b: tender_log[b]["oth_w"]), {}),
        ("Week Total",            col(lambda b: tender_log[b]["sup_w"] + tender_log[b]["oth_w"]), dict(bold=True)),
        ("MTD Supplier",          col(lambda b: tender_log[b]["sup_mtd"]), {}),
        ("MTD Other",             col(lambda b: tender_log[b]["oth_mtd"]), {}),
        ("Month Total",           col(lambda b: tender_log[b]["sup_mtd"] + tender_log[b]["oth_mtd"]), dict(bold=True)),
    ]),
    ("STOCK POSITION", [
        ("Sell Value", col(lambda b: stock[b]["sell"]), dict(bold=True)),
        ("Cost Value", col(lambda b: stock[b]["cost_val"]), dict(bold=True)),
        ("Margin %",   [margin_pct(stock[b]["sell"], stock[b]["cost_val"]) for b in BRANDS]
                       + [margin_pct(group_sell, group_cost)], dict(bold=True, pct=True)),
        ("Stock Cover (wks)", [stock_cover(stock[b]["sell"], sales[b][0]) for b in BRANDS]
                              + [stock_cover(group_sell, group_week_sales)], dict(decimal=True)),
    ]),
]


# ── CELL 10: Build workbook ────────────────────────────────────────────────
wb = Workbook()
ws = wb.active
ws.title = "Weekly Dashboard"

F_ = "Calibri"
thin = Side(style="thin", color="BFBFBF")
border = Border(left=thin, right=thin, top=thin, bottom=thin)
data_cols = [get_column_letter(i) for i in range(3, 3 + len(BRANDS) + 1)]
last_col = data_cols[-1]

for i, w in enumerate([4, 26] + [16] * (len(BRANDS) + 1), start=1):
    ws.column_dimensions[get_column_letter(i)].width = w

ws.merge_cells(f"B1:{last_col}1")
ws["B1"] = "GROUP - BRAND-WISE WEEKLY PERFORMANCE REPORT"
ws["B1"].font, ws["B1"].alignment = Font(name=F_, size=14, bold=True), Alignment(horizontal="center")
ws.merge_cells(f"B2:{last_col}2")
ws["B2"] = f"Week of {week_start:%d-%m-%Y} to {week_end:%d-%m-%Y} | " + " - ".join(b.title() for b in BRANDS)
ws["B2"].font, ws["B2"].alignment = Font(name=F_, size=10, italic=True), Alignment(horizontal="center")

row = 4
for c, label in zip(data_cols, BRANDS + ["GROUP TOTAL"]):
    cell = ws[f"{c}{row}"]
    cell.value, cell.font = label, Font(name=F_, bold=True)
    cell.alignment, cell.fill = Alignment(horizontal="center"), PatternFill("solid", start_color="BFBFBF")
row += 1

for section, metrics in LAYOUT:
    ws.merge_cells(f"B{row}:{last_col}{row}")
    ws[f"B{row}"] = section
    ws[f"B{row}"].font = Font(name=F_, bold=True)
    ws[f"B{row}"].fill = PatternFill("solid", start_color="D9D9D9")
    row += 1
    for label, values, opts in metrics:
        font = Font(name=F_, bold=opts.get("bold", False))
        ws[f"B{row}"], ws[f"B{row}"].font = label, font
        for c, v in zip(data_cols, values):
            cell = ws[f"{c}{row}"]
            if opts.get("pct"):
                cell.value, cell.number_format = round(v / 100, 4), "0.0%"
            elif opts.get("decimal"):
                cell.value, cell.number_format = round(v, 1), "0.0"
            else:
                cell.value, cell.number_format = round(v), "#,##0;-#,##0;-"
            cell.font, cell.alignment = font, Alignment(horizontal="right")
        row += 1
    row += 1

for r in ws.iter_rows(min_row=4, max_row=row - 2, min_col=2, max_col=len(BRANDS) + 3):
    for cell in r:
        cell.border = border

wb.save(OUT_PATH)
print(f"Saved {OUT_PATH}")


# ── CELL 11: Email ─────────────────────────────────────────────────────────
msg = MIMEMultipart()
msg["From"] = SMTP_USER
msg["To"] = ", ".join(EMAIL_TO)
msg["Subject"] = f"Weekly Brand Performance - {week_start:%d-%m-%Y} to {week_end:%d-%m-%Y}"
msg.attach(MIMEText("Attached is the weekly brand-wise performance dashboard.", "plain"))

with open(OUT_PATH, "rb") as f:
    part = MIMEBase("application", "octet-stream")
    part.set_payload(f.read())
encoders.encode_base64(part)
part.add_header("Content-Disposition", f'attachment; filename="Weekly_Dashboard_{week_end:%Y%m%d}.xlsx"')
msg.attach(part)

with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
    server.starttls()
    server.login(SMTP_USER, SMTP_PASSWORD)
    server.sendmail(SMTP_USER, EMAIL_TO, msg.as_string())
print("Email sent.")
