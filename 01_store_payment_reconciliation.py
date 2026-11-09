"""
Store Payment Reconciliation
============================
Daily Microsoft Fabric / PySpark notebook that reconciles in-store sales
captured on the e-commerce POS against what the payment providers actually
collected, per store and per day:

    POS CARD   vs  card-terminal CARD
    POS UPI    vs  card-terminal UPI  (+ UPI-aggregator UPI, optional)
    CASH       =   cash-register deposits (shown for reference)

It produces a styled Excel workbook with four sheets and emails it:
    * Report Day            - store-wise recon for the report day (T-1)
    * Consolidated          - store-wise recon for MTD or a rolling window
    * Order Level - Day     - order x payment-mode detail incl. implied cash
    * Order Level - Consol. - same, for the consolidated window

All brand, store, table, column, terminal and e-mail values in this file
are fictional placeholders.
"""

# ── CELL 1: Imports & config ───────────────────────────────────────────────
import os
import smtplib
from datetime import date, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import BytesIO

import pandas as pd

BRAND = "Banyan"
TZ = "Asia/Kolkata"
# FROM_UTC_TIMESTAMP assumes the session clock is UTC (Fabric default) - pin it.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ---- Report window -------------------------------------------------------
# REPORT_MONTH: set e.g. "2026-07" to rerun a past month; None = yesterday.
REPORT_MONTH = None
# CONSOLIDATED_MODE: "mtd" = month to date, "rolling" = last ROLLING_DAYS days
CONSOLIDATED_MODE = "mtd"
ROLLING_DAYS = 120

# ---- Behaviour flags -----------------------------------------------------
INCLUDE_CASH_IN_TOTAL = True   # add cash-register deposits into Total
USE_UPI_AGGREGATOR = True      # reconcile UPI against terminal + aggregator
SEND_EMAIL = True

# ---- Mail (never hard-code credentials; use env vars or Key Vault) -------
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
EMAIL_TO = [e.strip() for e in os.getenv("RECON_EMAIL_TO", "accounts@example.com").split(",") if e.strip()]
MAIL_CC = [e.strip() for e in os.getenv("RECON_EMAIL_CC", "").split(",") if e.strip()]
EMAIL_SUBJECT_PREFIX = f"{BRAND} Store Recon"
SIGN_OFF = "Analytics Team"

# ---- Source tables -------------------------------------------------------
ECOM_SCHEMA = "lake_refined.web_banyan"              # sale_headers, sale_ledger, tender_log
CARD_TERMINAL_TABLE = "lake_serve.mart.edc_settlements"
UPI_AGGREGATOR_TABLE = "lake_serve.mart.qr_collections"

STORE_ORDER = ["Downtown", "Harbor", "Hillview", "Lakeside", "Riverside"]

CASH_REGISTER_TABLES = {
    outlet: f"lake_serve.mart.till_cash_{outlet.lower()}" for outlet in STORE_ORDER
}

# POS location name -> canonical store. Source data has spelling variants
# and pop-up locations that roll into their parent store.
# Online, warehouse and blank locations are deliberately left unmapped.
STORE_NAME_MAP = {
    "Banyan Downtown":             "Downtown",
    "Banyan Downtown, Metro City": "Downtown",
    "Banyan Harbor":               "Harbor",
    "Banyan Hillview":             "Hillview",
    "Banyan Hillview, North":      "Hillview",
    "Banyan Lakeside":             "Lakeside",
    "Banyan Riverside":            "Riverside",
    "Banyan Pop-up Lakeside":      "Lakeside",
}

# UPI aggregator outlet name -> canonical store (all rows are UPI)
UPI_OUTLET_MAP = {
    "Banyan - DT":    "Downtown",
    "Banyan - HBR":   "Harbor",
    "Banyan - HV":    "Hillview",
    "Banyan - Floor2": "Lakeside",
    "Banyan - RS":    "Riverside",
}

# Card terminal ID -> store (many-to-one is fine: a store can have 2 terminals)
TERMINAL_MAP = {
    900101: "Downtown",
    900102: "Downtown",
    900103: "Harbor",
    900104: "Hillview",
    900105: "Lakeside",
    900106: "Lakeside",
    900107: "Riverside",
}

# POS tender source -> report payment mode
GATEWAY_MAP = {
    "card_edc": "CARD",
    "upi_qr":   "UPI",
    "coupon":   "COUPON",
}
# Coupon redemptions reversed on a refund arrive as POSITIVE amounts and must
# be negated. (Card/UPI reversals already arrive negative.)
SIGNED_REFUND_GATEWAYS = ["coupon"]

# Raw mode codes on the card-terminal settlement file -> report payment mode
TERMINAL_MODE_MAP = {"CRD": "CARD", "QR": "UPI"}

POS_MODES = ["CARD", "UPI", "COUPON"]
KEYS = ["outlet", "biz_day"]


# ── CELL 2: Resolve report window ──────────────────────────────────────────
def resolve_window():
    """Return (report_date, consolidated_start)."""
    today = date.today()
    if REPORT_MONTH:
        month_start = pd.to_datetime(REPORT_MONTH + "-01").date()
        next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = next_month - timedelta(days=1)
        if month_start == today.replace(day=1):
            # Month still in progress -> cap at yesterday
            month_end = min(month_end, today - timedelta(days=1))
        return month_end, month_start

    report_date = today - timedelta(days=1)
    if CONSOLIDATED_MODE == "rolling":
        return report_date, report_date - timedelta(days=ROLLING_DAYS)
    return report_date, report_date.replace(day=1)


report_date, consolidated_start = resolve_window()
report_date_str = report_date.strftime("%Y-%m-%d")
consolidated_start_str = consolidated_start.strftime("%Y-%m-%d")
print(f"Report day: {report_date_str} | Consolidated: {consolidated_start_str} -> {report_date_str}")


# ── CELL 3: SQL builders ───────────────────────────────────────────────────
def _case(col, mapping, else_value="NULL"):
    whens = "\n".join(f"                WHEN {col} = '{k}' THEN '{v}'" for k, v in mapping.items())
    return f"CASE\n{whens}\n                ELSE {else_value}\n            END"


def _payments_cte(name, amount_alias):
    """One row per order PER DAY per payment mode with the true paid amount."""
    mode_case = _case("p.tender_src", GATEWAY_MAP, "'OTHER'")
    signed = ", ".join(f"'{g}'" for g in SIGNED_REFUND_GATEWAYS)
    return f"""
    {name} AS (
        SELECT
            p.bill_ref,
            CAST(FROM_UTC_TIMESTAMP(p.logged_at, '{TZ}') AS DATE) AS txn_date,
            {mode_case} AS tender,
            SUM(
                CASE WHEN p.tender_src IN ({signed}) AND p.entry_kind = 'reversal'
                     THEN -p.amt_inr ELSE p.amt_inr END
            ) AS {amount_alias}
        FROM {ECOM_SCHEMA}.tender_log p
        WHERE p.state_flag = 'settled'
        GROUP BY 1, 2, 3
    )"""


def build_summary_query(date_filter_sql):
    """
    Store x day x payment-mode totals.

    Grouped by (bill_ref, day) on BOTH the sale-event side and the payment
    side, then joined on order AND date. An order can have activity on more
    than one day (advance today, balance next week, an edit or a return);
    collapsing to one row per order would dump every day's amount onto the
    first day and silently drop the later days.
    """
    store_case = _case("o.outlet_label", STORE_NAME_MAP)
    return f"""
    WITH order_scope AS (
        SELECT DISTINCT
            e.bill_ref,
            {store_case} AS outlet,
            CAST(FROM_UTC_TIMESTAMP(e.posted_at, '{TZ}') AS DATE) AS biz_day
        FROM {ECOM_SCHEMA}.sale_ledger e
        JOIN {ECOM_SCHEMA}.sale_headers o ON e.bill_ref = o.bill_ref
        WHERE o.outlet_label IS NOT NULL
    ),
    {_payments_cte("gateway_amounts", "gateway_amount")}
    SELECT
        os.outlet,
        os.biz_day,
        ga.tender,
        SUM(ga.gateway_amount) AS amt_inr
    FROM order_scope os
    JOIN gateway_amounts ga
        ON ga.bill_ref = os.bill_ref
       AND ga.txn_date = os.biz_day
    WHERE os.outlet IS NOT NULL
      AND ga.tender <> 'OTHER'
      AND os.biz_day {date_filter_sql}
    GROUP BY os.outlet, os.biz_day, ga.tender
    """


def build_order_level_query(date_filter_sql):
    """
    One row per order x day x payment mode actually used, plus one implied
    CASH row per order-day = that day's sale amount minus all non-cash
    payments that day (checked later against the cash register).

    PAYMENT_STAGE: orders with sale activity on more than one day get their
    first day labelled 'Advance' and later days 'Balance'.
    On an advance day the sale amount can be 0 while the advance payment
    still lands, which produces a NEGATIVE implied-cash row - expected.
    """
    store_case = _case("o.outlet_label", STORE_NAME_MAP)
    return f"""
    WITH order_sale AS (
        SELECT
            e.bill_ref,
            CAST(FROM_UTC_TIMESTAMP(e.posted_at, '{TZ}') AS DATE) AS biz_day,
            SUM(e.amt_inr) AS sold_inr
        FROM {ECOM_SCHEMA}.sale_ledger e
        GROUP BY 1, 2
    ),
    order_sale_staged AS (
        SELECT
            *,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY bill_ref) = 1 THEN 'Single'
                WHEN ROW_NUMBER() OVER (PARTITION BY bill_ref ORDER BY biz_day) = 1
                    THEN 'Advance'
                ELSE 'Balance'
            END AS payment_stage
        FROM order_sale
    ),
    order_location AS (
        SELECT o.bill_ref, o.receipt_no, {store_case} AS outlet
        FROM {ECOM_SCHEMA}.sale_headers o
    ),
    {_payments_cte("order_modes", "tender_inr")},
    order_noncash_total AS (
        SELECT bill_ref, txn_date, SUM(tender_inr) AS noncash_total
        FROM order_modes
        WHERE tender <> 'OTHER'
        GROUP BY bill_ref, txn_date
    )

    -- Non-cash rows
    SELECT
        os.bill_ref, ol.receipt_no, os.sold_inr, ol.outlet,
        os.payment_stage, om.tender, om.tender_inr AS amt_inr, os.biz_day
    FROM order_sale_staged os
    LEFT JOIN order_location ol ON ol.bill_ref = os.bill_ref
    JOIN order_modes om
        ON om.bill_ref = os.bill_ref
       AND om.txn_date = os.biz_day
    WHERE om.tender <> 'OTHER'
      AND ol.outlet IS NOT NULL
      AND os.biz_day {date_filter_sql}

    UNION ALL

    -- Implied cash rows (only when non-zero; covers fully-cash order-days)
    SELECT
        os.bill_ref, ol.receipt_no, os.sold_inr, ol.outlet,
        os.payment_stage, 'CASH' AS tender,
        ROUND(os.sold_inr - COALESCE(ont.noncash_total, 0), 2) AS amt_inr,
        os.biz_day
    FROM order_sale_staged os
    LEFT JOIN order_location ol ON ol.bill_ref = os.bill_ref
    LEFT JOIN order_noncash_total ont
        ON ont.bill_ref = os.bill_ref
       AND ont.txn_date = os.biz_day
    WHERE ol.outlet IS NOT NULL
      AND os.biz_day {date_filter_sql}
      AND ROUND(os.sold_inr - COALESCE(ont.noncash_total, 0), 2) <> 0.00

    ORDER BY biz_day DESC, receipt_no, tender
    """


# ── CELL 4: Run POS queries ────────────────────────────────────────────────
day_filter = f"= '{report_date_str}'"
range_filter = f"BETWEEN '{consolidated_start_str}' AND '{report_date_str}'"

pos_day_pd = spark.sql(build_summary_query(day_filter)).toPandas()
pos_cons_pd = spark.sql(build_summary_query(range_filter)).toPandas()
orders_day_pd = spark.sql(build_order_level_query(day_filter)).toPandas()
orders_cons_pd = spark.sql(build_order_level_query(range_filter)).toPandas()

print(f"POS rows  day={len(pos_day_pd)} consolidated={len(pos_cons_pd)}")
print(f"Order rows day={len(orders_day_pd)} consolidated={len(orders_cons_pd)}")


# ── CELL 5: Card terminal (CARD + UPI) ─────────────────────────────────────
terminal_pd = spark.sql(f"""
    SELECT edc_id, edc_mode, amt_inr, edc_ts
    FROM {CARD_TERMINAL_TABLE}
    WHERE LOWER(edc_state) = 'approved'
""").toPandas()

terminal_pd["outlet"] = terminal_pd["edc_id"].map(TERMINAL_MAP)
terminal_pd = terminal_pd[terminal_pd["outlet"].notna()].copy()   # drop other brands' terminals
terminal_pd["biz_day"] = pd.to_datetime(terminal_pd["edc_ts"], errors="coerce").dt.date
terminal_pd["tender"] = terminal_pd["edc_mode"].astype(str).str.strip().str.upper().map(TERMINAL_MODE_MAP)
terminal_pd = terminal_pd[terminal_pd["tender"].notna()]

terminal_agg = (
    terminal_pd.groupby(KEYS + ["tender"], as_index=False)
    .agg(edc_inr=("amt_inr", "sum"))
)
print(f"Terminal rows: {len(terminal_agg)}")


# ── CELL 6: UPI aggregator (optional second UPI source) ───────────────────
if USE_UPI_AGGREGATOR:
    upi_pd = spark.sql(f"""
        SELECT qr_outlet, qr_ts, amt_inr
        FROM {UPI_AGGREGATOR_TABLE}
    """).toPandas()
    upi_pd["outlet"] = upi_pd["qr_outlet"].map(UPI_OUTLET_MAP)
    upi_pd = upi_pd[upi_pd["outlet"].notna()].copy()
    upi_pd["biz_day"] = pd.to_datetime(upi_pd["qr_ts"], errors="coerce").dt.date
    upi_agg = upi_pd.groupby(KEYS, as_index=False).agg(AGG_UPI=("amt_inr", "sum"))
else:
    upi_agg = pd.DataFrame(columns=KEYS + ["AGG_UPI"])
print(f"UPI aggregator rows: {len(upi_agg)}")


# ── CELL 7: Cash register deposits ─────────────────────────────────────────
cash_frames = []
for outlet, table in CASH_REGISTER_TABLES.items():
    pdf = spark.sql(f"SELECT posting_day, deposit_inr FROM {table}").toPandas()
    pdf["outlet"] = outlet
    cash_frames.append(pdf)

cash_pd = pd.concat(cash_frames, ignore_index=True)
cash_pd["biz_day"] = pd.to_datetime(cash_pd["posting_day"], errors="coerce").dt.date
cash_agg = cash_pd.groupby(KEYS, as_index=False).agg(CASH=("deposit_inr", "sum"))
print(f"Cash register rows: {len(cash_agg)}")


# ── CELL 8: Merge & compute differences ────────────────────────────────────
RECON_COLUMNS = (
    ["Date", "Store", "CASH"] + POS_MODES + ["Total", "TERM_CARD", "TERM_UPI"]
    + (["AGG_UPI"] if USE_UPI_AGGREGATOR else [])
    + ["UPI_DIFF", "CARD_DIFF", "TOTAL_DIFF"]
)


def _wide(df, value_col, columns, rename=None):
    if df.empty:
        out = pd.DataFrame(columns=KEYS)
    else:
        out = df.pivot_table(index=KEYS, columns="tender", values=value_col,
                             aggfunc="sum", fill_value=0).reset_index()
        out.columns.name = None
    if rename:
        out = out.rename(columns=rename)
    for c in columns:
        if c not in out.columns:
            out[c] = 0.0
    return out


def build_recon_table(pos_long):
    if pos_long.empty:
        return pd.DataFrame(columns=RECON_COLUMNS)

    pos = _wide(pos_long, "amt_inr", POS_MODES)
    term = _wide(terminal_agg, "edc_inr", ["TERM_CARD", "TERM_UPI"],
                 rename={"CARD": "TERM_CARD", "UPI": "TERM_UPI"})

    merged = (
        pos.merge(term[KEYS + ["TERM_CARD", "TERM_UPI"]], on=KEYS, how="left")
           .merge(upi_agg, on=KEYS, how="left")
           .merge(cash_agg, on=KEYS, how="left")
    )
    for c in ["TERM_CARD", "TERM_UPI", "AGG_UPI", "CASH"]:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0.0)

    merged["Total"] = merged[POS_MODES].sum(axis=1)
    if INCLUDE_CASH_IN_TOTAL:
        merged["Total"] += merged["CASH"]

    upi_collected = merged["TERM_UPI"] + (merged["AGG_UPI"] if USE_UPI_AGGREGATOR else 0)
    merged["UPI_DIFF"] = merged["UPI"] - upi_collected
    merged["CARD_DIFF"] = merged["CARD"] - merged["TERM_CARD"]
    merged["TOTAL_DIFF"] = merged["UPI_DIFF"] + merged["CARD_DIFF"]

    merged = merged.rename(columns={"outlet": "Store", "biz_day": "Date"})
    merged["_sort"] = merged["Store"].apply(lambda s: STORE_ORDER.index(s) if s in STORE_ORDER else 99)
    merged = merged.sort_values(["Date", "_sort"], ascending=[False, True])
    return merged[RECON_COLUMNS]


recon_day = build_recon_table(pos_day_pd)
recon_cons = build_recon_table(pos_cons_pd)


# ── CELL 9: Excel builder ──────────────────────────────────────────────────
from openpyxl.styles import Alignment, Border, Font, PatternFill
from openpyxl.styles.borders import Side
from openpyxl.utils import get_column_letter

THIN = Border(*(Side(style="thin"),) * 4)
FILL = lambda hex_: PatternFill(start_color=hex_, end_color=hex_, fill_type="solid")


def write_sheet(ws, df, money_cols, header_fill_for, diff_cols=(), freeze="A2", max_width=22):
    if df.empty:
        ws.cell(row=1, column=1, value="No data available for this period.")
        return

    headers = list(df.columns)
    for ci, name in enumerate(headers, 1):
        cell = ws.cell(row=1, column=ci, value=name)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = header_fill_for(name)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = THIN
    ws.row_dimensions[1].height = 28

    ok_fill, bad_fill = FILL("E2EFDA"), FILL("FCE4E4")
    for ri, row in enumerate(df.itertuples(index=False), 2):
        for ci, (name, val) in enumerate(zip(headers, row), 1):
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.border = THIN
            if name == "Date":
                cell.number_format = "yyyy-mm-dd"
            elif name in money_cols:
                cell.number_format = "#,##0.00"
            if name in diff_cols:
                cell.fill = ok_fill if abs(val) < 1 else bad_fill

    for ci, name in enumerate(headers, 1):
        width = max(len(str(name)), df[name].astype(str).map(len).max())
        ws.column_dimensions[get_column_letter(ci)].width = min(width + 3, max_width)

    ws.freeze_panes = freeze
    ws.auto_filter.ref = ws.dimensions


def recon_header_fill(name):
    if name in ("TERM_CARD", "TERM_UPI", "AGG_UPI"):
        return FILL("1F4E79")      # provider side
    if name.endswith("_DIFF"):
        return FILL("C00000")      # differences
    return FILL("5B2C6F")          # POS side


ORDER_COLS = {
    "bill_ref": "Bill Ref", "receipt_no": "Receipt No", "sold_inr": "Bill Value",
    "outlet": "Store", "payment_stage": "Stage", "tender": "Tender",
    "amt_inr": "Tender Value", "biz_day": "Date",
}


def build_excel():
    out = BytesIO()
    money = [c for c in RECON_COLUMNS if c not in ("Date", "Store")]
    diffs = ["UPI_DIFF", "CARD_DIFF", "TOTAL_DIFF"]
    sheets = {
        "Report Day":            (recon_day, money, recon_header_fill, diffs, "C2", 22),
        "Consolidated":          (recon_cons, money, recon_header_fill, diffs, "C2", 22),
        "Order Level - Day":     (orders_day_pd.rename(columns=ORDER_COLS)[list(ORDER_COLS.values())],
                                  ["Sale Amount", "Mode Amount"], lambda _: FILL("404040"), (), "A2", 30),
        "Order Level - Consol.": (orders_cons_pd.rename(columns=ORDER_COLS)[list(ORDER_COLS.values())],
                                  ["Sale Amount", "Mode Amount"], lambda _: FILL("404040"), (), "A2", 30),
    }
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for name, (df, money_cols, fill_fn, diff_cols, freeze, width) in sheets.items():
            pd.DataFrame().to_excel(writer, sheet_name=name, index=False)
            write_sheet(writer.sheets[name], df, money_cols, fill_fn, diff_cols, freeze, width)
    out.seek(0)
    return out.getvalue()


excel_bytes = build_excel()
print(f"Excel built: {len(excel_bytes):,} bytes")


# ── CELL 10: Email ─────────────────────────────────────────────────────────
def send_report(payload: bytes):
    if not (SMTP_USER and SMTP_PASSWORD):
        raise RuntimeError("SMTP_USER / SMTP_PASSWORD not set - see .env.example")

    window_label = "month-to-date" if CONSOLIDATED_MODE == "mtd" else f"rolling {ROLLING_DAYS}-day"
    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(EMAIL_TO)
    if MAIL_CC:
        msg["Cc"] = ", ".join(MAIL_CC)
    msg["Subject"] = f"{EMAIL_SUBJECT_PREFIX} - {report_date:%d %b %Y}"

    body = (
        "Dear Team,\n\n"
        f"Please find attached the {BRAND} store reconciliation for {report_date:%d-%b-%Y}.\n\n"
        "Sheets:\n"
        "  - Report Day: store-wise reconciliation for the day\n"
        f"  - Consolidated: {window_label} reconciliation summary\n"
        "  - Order Level - Day: order-level detail incl. payment stage\n"
        f"  - Order Level - Consol.: order-level detail for the {window_label} window\n\n"
        + ("Note: UPI is reconciled against terminal UPI + aggregator UPI combined.\n\n"
           if USE_UPI_AGGREGATOR else "")
        + f"Regards,\n{SIGN_OFF}"
    )
    msg.attach(MIMEText(body, "plain"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(payload)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition",
                    f'attachment; filename="{BRAND}_Store_Recon_{report_date:%Y%m%d}.xlsx"')
    msg.attach(part)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, EMAIL_TO + MAIL_CC, msg.as_string())
    print(f"Email sent to {EMAIL_TO}" + (f" | CC {MAIL_CC}" if MAIL_CC else ""))


if SEND_EMAIL:
    send_report(excel_bytes)
