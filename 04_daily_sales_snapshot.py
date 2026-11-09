"""
Daily Sales Snapshot vs Target
==============================
Fabric / PySpark notebook that emails store leads a daily HTML snapshot per
brand: yesterday's sale, daily / MTD / whole-month target, MTD achieved,
shortfall, the daily run-rate needed for the rest of the week, and Google
review counts.

Logic worth noting:
  * Freshness gate - the notebook stops if yesterday's ERP data or today's
    e-commerce loads haven't landed, so nobody gets a half-empty report.
  * Messy location strings are normalised to canonical stores with ordered,
    brand-aware rules.
  * Wide target sheets (one column per store) are unpivoted with stack().
  * Targets for a brand + its sub-brand are summed per store.
  * A temporary pop-up outlet gets its own column only when it has a
    target this month.
  * Shortfall is spread across the days left in the week (Sun=7 ... Sat=1).

All brand, store, table and e-mail values are fictional placeholders.
"""

# ── CELL 1: Config ─────────────────────────────────────────────────────────
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import pytz
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TZ = "Asia/Kolkata"
# FROM_UTC_TIMESTAMP assumes the session clock is UTC (Fabric default) - pin it.
spark.conf.set("spark.sql.session.timeZone", "UTC")
STORES = ["Downtown", "Harbor", "Hillview", "Lakeside", "Riverside", "Bayfront"]
POPUP_STORE = "Pop-up"
POPUP_METRO = "metro city"          # which pop-up's target to use this month
SHOW_GOOGLE_REVIEWS = True

ERP_ORDERS = "lake_refined.till_erp.bill_headers"
UNIFIED_ORDERS = "lake_refined.unified.all_bills"        # ERP + e-com, one table
LOAD_LOG = "lake_refined.mart.ingest_audit"
RATINGS_TABLE = "lake_refined.voc.place_ratings"
TARGETS = {
    "banyan": "lake_refined.mart.day_goal_banyan",
    "maple":  "lake_refined.mart.day_goal_maple",
    "lumen":  "lake_refined.mart.day_goal_lumen",
    "popup":  "lake_refined.mart.popup_goals",
}
REQUIRED_LOADS = {"tables": ["sale_ledger", "sale_headers"], "schemas": ["web_banyan", "web_maple"]}

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
EMAIL_TO = [e for e in os.getenv("SNAPSHOT_EMAIL_TO", "store-leads@example.com").split(",") if e]
SIGN_OFF = "Analytics Team"

yesterday = (datetime.now(pytz.timezone(TZ)) - timedelta(days=1)).date()
DAYS_LEFT = {"Sunday": 7, "Monday": 6, "Tuesday": 5, "Wednesday": 4,
             "Thursday": 3, "Friday": 2, "Saturday": 1}[yesterday.strftime("%A")]
print(f"Report date {yesterday} ({yesterday:%A}) | days left in week: {DAYS_LEFT}")


# ── CELL 2: Freshness gate ─────────────────────────────────────────────────
latest_erp_day = spark.sql(f"""
    SELECT CAST(MAX(billed_at) AS DATE) AS d
    FROM {ERP_ORDERS}
    WHERE till_user <> 'sysadmin' AND till_label NOT LIKE '%(retired)%'
""").collect()[0]["d"]

loaded_today = {
    r["obj_label"]
    for r in spark.sql(f"""
        SELECT obj_label
        FROM {LOAD_LOG}
        WHERE obj_label IN ({",".join(f"'{t}'" for t in REQUIRED_LOADS["tables"])})
          AND zone_name IN ({",".join(f"'{s}'" for s in REQUIRED_LOADS["schemas"])})
          AND state_flag = 'ok'
          AND CAST(landed_at AS DATE) = CAST(FROM_UTC_TIMESTAMP(current_timestamp(), '{TZ}') AS DATE)
    """).collect()
}
missing = [t for t in REQUIRED_LOADS["tables"] if t not in loaded_today]

if missing or latest_erp_day != yesterday:
    print(f"Data not ready - ERP latest={latest_erp_day}, missing loads={missing}. Stopping.")
    mssparkutils.session.stop()
print("Freshness checks passed.")


# ── CELL 3: Store normalisation ────────────────────────────────────────────
def normalize_store(col, brand_hint=None):
    """
    Map free-text location names to canonical stores. Rules are ordered: the
    first match wins, so brand-qualified rules go before generic city rules.
    """
    c = F.lower(col)
    expr = F.when(c.contains("pop-up") | c.contains("popup"), POPUP_STORE)
    if brand_hint:
        for outlet in STORES:
            expr = expr.when(c.contains(brand_hint) & c.contains(outlet.lower()), outlet)
    aliases = {
        "Downtown":  ["downtown", "dt "],
        "Harbor":    ["harbor", "harbour"],
        "Hillview":  ["hillview", "hill view"],
        "Lakeside":  ["lakeside"],
        "Riverside": ["riverside"],
        "Bayfront":  ["bayfront", "bay front"],
    }
    for outlet, keys in aliases.items():
        cond = None
        for k in keys:
            cond = c.contains(k) if cond is None else cond | c.contains(k)
        expr = expr.when(cond, outlet)
    return expr.otherwise(F.lit(None))


# ── CELL 4: Sales ──────────────────────────────────────────────────────────
sale_headers = spark.read.table(UNIFIED_ORDERS)


def sales_frames(df, date_col, amt_col):
    clean = df.withColumn("bill_day", F.to_date(date_col)).filter(F.col("outlet").isNotNull())
    today = (clean.filter(F.col("bill_day") == F.lit(yesterday))
             .groupBy("outlet").agg(F.sum(amt_col).alias("todays_sale")))
    mtd = (clean.filter((F.col("bill_day") >= F.trunc(F.lit(yesterday), "month")) &
                        (F.col("bill_day") <= F.lit(yesterday)))
           .groupBy("outlet").agg(F.sum(amt_col).alias("mtd_done")))
    return today, mtd


banyan_sales = sales_frames(
    sale_headers.filter(F.lower("banner").contains("banyan") | F.lower("outlet_label").contains("pop-up"))
          .withColumn("outlet", normalize_store(F.col("outlet_label"), brand_hint="banyan")),
    "logged_at", "gross_inr")

maple_events = spark.sql("""
    SELECT e.posted_at, e.amt_inr, o.outlet_label
    FROM lake_refined.web_maple.sale_ledger e
    JOIN lake_refined.web_maple.sale_headers o ON e.bill_ref = o.bill_ref
""").withColumn("posted_at", F.from_utc_timestamp("posted_at", TZ))
maple_sales = sales_frames(
    maple_events.withColumn("outlet", normalize_store(F.col("outlet_label"))),
    "posted_at", "amt_inr")


# ── CELL 5: Targets ────────────────────────────────────────────────────────
def unpivot_targets(table):
    """Wide sheet (goal_day, <one column per outlet>) -> long (goal_day, outlet, day_goal)."""
    df = spark.read.table(table)
    pairs = ", ".join(f"'{s}', `{s}`" for s in STORES if s in df.columns)
    n = sum(1 for s in STORES if s in df.columns)
    return df.select("goal_day", F.expr(f"stack({n}, {pairs}) AS (outlet, day_goal)"))


def target_frames(long_df):
    month = long_df.filter((F.year("goal_day") == yesterday.year) &
                           (F.month("goal_day") == yesterday.month))
    daily = (month.filter(F.col("goal_day") == F.lit(yesterday))
             .groupBy("outlet").agg(F.sum("day_goal").alias("day_goal")))
    mtd = (month.filter(F.col("goal_day") <= F.lit(yesterday))
           .groupBy("outlet").agg(F.sum("day_goal").alias("mtd_target")))
    whole = month.groupBy("outlet").agg(F.sum("day_goal").alias("month_target"))
    return daily, mtd, whole


popup_long = (spark.read.table(TARGETS["popup"])
             .filter(F.lower("metro") == POPUP_METRO)
             .select("goal_day", F.lit(POPUP_STORE).alias("outlet"), "day_goal"))

banyan_targets = target_frames(unpivot_targets(TARGETS["banyan"]).unionByName(popup_long))
# Maple store targets = Maple + its Lumen sub-brand, summed per store
maple_targets = target_frames(unpivot_targets(TARGETS["maple"]).unionByName(unpivot_targets(TARGETS["lumen"])))


# ── CELL 6: Google reviews ─────────────────────────────────────────────────
feedback_rows = (spark.read.table(RATINGS_TABLE)
           .withColumn("rating_day", F.to_date(F.substring("rated_at", 1, 10)))
           .filter(F.col("rating_day") == F.lit(yesterday)))


def review_counts(banner):
    return (feedback_rows.filter(F.lower("outlet_label").contains(banner))
            .withColumn("outlet", normalize_store(F.col("outlet_label"), brand_hint=banner))
            .filter(F.col("outlet").isNotNull())
            .groupBy("outlet").agg(F.count("*").alias("rating_count")))


# ── CELL 7: Build snapshot ─────────────────────────────────────────────────
w_all = Window.partitionBy()


def build_snapshot(targets, sales, reviews_df):
    daily, mtd_t, whole = targets
    today, mtd_s = sales
    df = (daily.join(whole, "outlet", "left").join(mtd_t, "outlet", "left")
          .join(today, "outlet", "left").join(mtd_s, "outlet", "left")
          .join(reviews_df, "outlet", "left")
          .fillna(0, ["todays_sale", "mtd_done", "rating_count"]))

    return (df
        .withColumn("short_fall", F.col("mtd_target") - F.col("mtd_done"))
        .withColumn("shortfall_per_day", F.col("short_fall") / F.lit(DAYS_LEFT))
        .withColumn("on_track_daily", F.col("day_goal") + F.col("shortfall_per_day"))
        .withColumn("mtd_target_pct", F.round(F.col("mtd_target") / F.col("month_target") * 100, 0))
        .withColumn("mtd_achieved_pct", F.round(F.col("mtd_done") / F.col("month_target") * 100, 0))
        # Group totals carried on every row (window over all rows)
        .withColumn("tot_month_target", F.sum("month_target").over(w_all))
        .withColumn("tot_mtd_target", F.sum("mtd_target").over(w_all))
        .withColumn("tot_mtd_achieved", F.sum("mtd_done").over(w_all))
        .withColumn("tot_today", F.sum("todays_sale").over(w_all))
        .withColumn("tot_short_fall", F.col("tot_mtd_target") - F.col("tot_mtd_achieved"))
        .withColumn("tot_mtd_target_pct", F.round(F.col("tot_mtd_target") / F.col("tot_month_target") * 100, 0))
        .withColumn("tot_mtd_achieved_pct", F.round(F.col("tot_mtd_achieved") / F.col("tot_month_target") * 100, 0))
        # Display rounding for the headline target
        .withColumn("month_target_display", F.round(F.col("month_target") / 10000) * 10000)
        .withColumn("tot_month_target_display", F.round(F.col("tot_month_target") / 10000) * 10000))


banyan = build_snapshot(banyan_targets, banyan_sales, review_counts("banyan")).toPandas()
maple = build_snapshot(maple_targets, maple_sales, review_counts("maple")).toPandas()


# ── CELL 8: HTML ───────────────────────────────────────────────────────────
HL = "background:#fff3a0;font-weight:bold;"


def cells(values, total, fmt="num", style=""):
    def one(v):
        if pd.isna(v):
            return f"<td style='{style}'>-</td>"
        txt = f"{int(round(float(v)))}%" if fmt == "pct" else f"{int(round(float(v))):,}"
        return f"<td style='{style}'>{txt}</td>"
    return "".join(one(v) for v in values) + one(total)


def brand_table(name, pdf, stores):
    pdf = pdf.set_index(pdf["outlet"].str.strip()).reindex(stores)
    t = lambda c: pdf[c].dropna().iloc[0] if pdf[c].notna().any() else 0
    span = len(stores) + 2

    rows = [
        ("Target",                     cells(pdf["month_target_display"], t("tot_month_target_display"))),
        ("Today's Sale",               cells(pdf["todays_sale"], t("tot_today"))),
        ("Daily Target",               cells(pdf["day_goal"], pdf["day_goal"].sum())),
        ("MTD Target",                 cells(pdf["mtd_target"], t("tot_mtd_target"))),
        ("MTD Achieved",               cells(pdf["mtd_done"], t("tot_mtd_achieved"), style=HL)),
        ("Shortfall",                  cells(pdf["short_fall"], t("tot_short_fall"))),
        ("Daily Target to be on Track", cells(pdf["on_track_daily"], pdf["on_track_daily"].sum())),
        ("MTD Target %",               cells(pdf["mtd_target_pct"], t("tot_mtd_target_pct"), fmt="pct")),
        ("MTD % Achieved",             cells(pdf["mtd_achieved_pct"], t("tot_mtd_achieved_pct"), fmt="pct")),
        (None, None),
        ("Shortfall per Remaining Day", cells(pdf["shortfall_per_day"], pdf["shortfall_per_day"].sum())),
        ("Days Left in Week",          cells([DAYS_LEFT] * len(stores), DAYS_LEFT)),
    ]
    if SHOW_GOOGLE_REVIEWS:
        rows += [(None, None),
                 ("Google Reviews", cells(pdf["rating_count"].fillna(0), pdf["rating_count"].sum()))]

    body = "".join(
        f'<tr class="gap"><td colspan="{span}"></td></tr>' if label is None
        else f'<tr><td class="label">{label}</td>{html}</tr>'
        for label, html in rows
    )
    head = "".join(f"<th>{s}</th>" for s in stores)
    return (f'<table><tr><td class="header" colspan="{span}">{name.upper()}</td></tr>'
            f"<tr><th>Snapshot</th>{head}<th>Overall</th></tr>{body}</table><br>")


banyan_stores = STORES + ([POPUP_STORE] if POPUP_STORE in set(banyan["outlet"]) else [])


def headline(name, pdf):
    if pdf.empty:
        return ""
    return (f"<p>{name} is at <b>{int(pdf['tot_mtd_achieved_pct'].fillna(0).iloc[0])}%</b> MTD "
            f"against a month-to-date target of <b>{int(pdf['tot_mtd_target_pct'].fillna(0).iloc[0])}%</b>.</p>")


html_body = f"""
<html><head><style>
body {{ font-family: Arial, sans-serif; }}
table {{ border-collapse: collapse; width: 100%; margin-bottom: 24px; }}
th, td {{ border: 1px solid #333; padding: 6px; font-size: 13px; text-align: center; }}
th, .label, .header {{ background: #2E4057; color: #fff; }}
.label {{ text-align: left; width: 240px; }}
.header {{ font-weight: bold; }}
.gap td {{ border: none; height: 14px; }}
</style></head><body>
<p><b>Sales snapshot for {yesterday:%d %B %Y} ({yesterday:%A}).</b></p>
{headline("Banyan", banyan)}
{headline("Maple", maple)}
{brand_table("Banyan", banyan, banyan_stores)}
{brand_table("Maple", maple, STORES)}
<p>Regards,<br><b>{SIGN_OFF}</b></p>
<p style="font-size:12px;color:#777;"><i>Auto-generated from Microsoft Fabric.</i></p>
</body></html>
"""


# ── CELL 9: Send ───────────────────────────────────────────────────────────
msg = MIMEMultipart("alternative")
msg["From"] = SMTP_USER
msg["To"] = ", ".join(EMAIL_TO)
msg["Subject"] = f"Sales Snapshot - Banyan & Maple - {yesterday:%d %b %Y}"
msg.attach(MIMEText(html_body, "html"))

with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
    server.starttls()
    server.login(SMTP_USER, SMTP_PASSWORD)
    server.sendmail(SMTP_USER, EMAIL_TO, msg.as_string())
print("Email sent.")
