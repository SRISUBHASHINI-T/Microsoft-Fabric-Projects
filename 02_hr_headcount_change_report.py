"""
HR Headcount Change Report
==========================
Fabric / PySpark notebook that:
  1. Loads one HR master table per legal entity and harmonises their schemas
     (inconsistent column names, spaces, punctuation).
  2. Unions them into a single current snapshot (one row per entity + employee ID).
  3. Compares against the snapshot saved on the previous run:
       - entity-level headcount change
       - department-level change
       - employees added / removed
  4. Builds pivot tables (entity x location / status / designation, dept x entity)
     with row and column totals.
  5. Emails an HTML report, and ONLY after a successful send overwrites the
     snapshot table so the next run compares against what people last saw.

All entity, location, table and e-mail values are fictional placeholders.
"""

# ── CELL 1: Config ─────────────────────────────────────────────────────────
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from pyspark.sql import functions as F

HR_TABLES = {
    "lake_serve.mart.staff_roster_bny":  "BNY",
    "lake_serve.mart.staff_roster_cdr":  "CDR",
    "lake_serve.mart.staff_roster_mpl":  None,     # table already carries Entity (two sub-entities)
}
SNAPSHOT_TABLE = "lake_serve.mart.roster_snapshot_hist"
KEY = ["LegalCo", "StaffCode"]
DETAIL_COLS = ["StaffCode", "LegalCo", "StaffName", "Grade", "BizUnit", "Worksite"]

ENTITY_ORDER = ["BNY", "CDR", "MPL - MAPLE", "MPL - WILLOW"]
LOCATION_ORDER = ["DOWNTOWN", "HARBOR", "HILLVIEW", "LAKESIDE", "RIVERSIDE", "HEAD OFFICE", "WAREHOUSE"]
DESIGNATION_ORDER = [
    "INTERN", "ANALYST", "SENIOR ANALYST", "TEAM LEAD", "LEAD II",
    "PRINCIPAL", "HEAD", "DIRECTOR", "VP", "CEO",
]

# Source sheets spell the same field differently - map them to one name.
RENAME_MAP = {
    "Confirmed On":       "ConfirmedOn",
    "Confirm Dt":         "ConfirmedOn",
    "Row #":              "RowNo",
    "Staff Full Name":    "StaffName",
    "Staff Nm":           "StaffName",
    "Staff Code":         "StaffCode",
    "StaffCd":            "StaffCode",
    "Civil Status":       "CivilStatus",
    "Civl Status":        "CivilStatus",   # typo seen in one source sheet
    "Legal Company":      "LegalCo",
    "Work Site":          "Worksite",
    "Business Unit":      "BizUnit",
    "Employment Type":    "EmpType",
}

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
EMAIL_TO = [e for e in os.getenv("HR_EMAIL_TO", "hr-leads@example.com").split(",") if e]
MAIL_CC = [e for e in os.getenv("HR_EMAIL_CC", "").split(",") if e]
SIGN_OFF = "Analytics Team"
MAX_LISTED_EMPLOYEES = 20


# ── CELL 2: Load & harmonise ───────────────────────────────────────────────
def clean_columns(df):
    for old, new in RENAME_MAP.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    for col in df.columns:
        clean = col.strip()
        for ch in (" ", ".", "-", "/"):
            clean = clean.replace(ch, "_")
        if clean != col:
            df = df.withColumnRenamed(col, clean)
    return df


frames = []
for table, entity in HR_TABLES.items():
    df = clean_columns(spark.read.table(table))
    if entity is not None:
        df = df.withColumn("LegalCo", F.lit(entity))
    frames.append(df)

combined_df = frames[0]
for df in frames[1:]:
    combined_df = combined_df.unionByName(df, allowMissingColumns=True)

# Trim text keys, then enforce one current record per employee.
for c in ["LegalCo", "BizUnit", "Worksite", "Grade"]:
    combined_df = combined_df.withColumn(c, F.trim(F.col(c)))
combined_df = combined_df.dropDuplicates(KEY).persist()
print(f"Current employees: {combined_df.count()}")

current_counts = {
    r["LegalCo"]: r["n"]
    for r in combined_df.groupBy("LegalCo").agg(F.countDistinct("StaffCode").alias("n")).collect()
}
print("Current counts:", current_counts)


# ── CELL 3: Load previous snapshot ─────────────────────────────────────────
previous_df = None
if spark.catalog.tableExists(SNAPSHOT_TABLE):
    previous_df = spark.read.table(SNAPSHOT_TABLE).select(*DETAIL_COLS).persist()
    if previous_df.count() == 0:
        previous_df = None
first_run = previous_df is None
print("First run - no comparison." if first_run else "Previous snapshot found.")


# ── CELL 4: Compare ────────────────────────────────────────────────────────
def label_change(x):
    return "Increase" if x > 0 else "Decrease" if x < 0 else "No change"


comparison = {}
if not first_run:
    prev_counts = {
        r["LegalCo"]: r["n"]
        for r in previous_df.groupBy("LegalCo").agg(F.countDistinct("StaffCode").alias("n")).collect()
    }
    rows = [
        {"LegalCo": e, "Previous": prev_counts.get(e, 0), "Current": current_counts.get(e, 0)}
        for e in ENTITY_ORDER
    ]
    ent = pd.DataFrame(rows)
    ent.loc[len(ent)] = ["TOTAL", ent["Previous"].sum(), ent["Current"].sum()]
    ent["Change"] = ent["Current"] - ent["Previous"]
    ent["Trend"] = ent["Change"].apply(label_change)
    comparison["entity"] = ent

    # Added / removed - compared on the composite key, so an ID reused
    # across entities is not mistaken for the same person.
    cur_keys = combined_df.select(*DETAIL_COLS)
    comparison["added"] = cur_keys.join(previous_df.select(*KEY), KEY, "left_anti").toPandas()
    comparison["removed"] = previous_df.join(cur_keys.select(*KEY), KEY, "left_anti").toPandas()

    cur_dept = combined_df.groupBy("LegalCo", "BizUnit").agg(F.countDistinct("StaffCode").alias("Current"))
    prev_dept = previous_df.groupBy("LegalCo", "BizUnit").agg(F.countDistinct("StaffCode").alias("Previous"))
    dept = (
        cur_dept.join(prev_dept, ["LegalCo", "BizUnit"], "full_outer")
        .fillna(0, ["Current", "Previous"])
        .withColumn("Change", F.col("Current") - F.col("Previous"))
        .filter((F.col("BizUnit").isNotNull()) & (F.col("BizUnit") != "") & (F.col("Change") != 0))
        .orderBy("LegalCo", F.abs("Change").desc())
        .toPandas()
    )
    dept["Trend"] = dept["Change"].apply(label_change)
    comparison["dept"] = dept


# ── CELL 5: Summary pivots ─────────────────────────────────────────────────
def headcount_by(*cols):
    return (
        combined_df.groupBy(*cols)
        .agg(F.countDistinct("StaffCode").alias("Headcount"))
        .toPandas()
    )


def pivot_with_totals(df, index_col, column_col, index_order=None, column_order=None):
    pv = df.pivot_table(index=index_col, columns=column_col, values="Headcount",
                        aggfunc="sum", fill_value=0)
    if index_order:
        pv = pv.reindex([i for i in index_order if i in pv.index] +
                        [i for i in pv.index if i not in index_order])
    if column_order:
        pv = pv[[c for c in column_order if c in pv.columns] +
                [c for c in pv.columns if c not in column_order]]
    pv["Total"] = pv.sum(axis=1)
    pv.loc["Total"] = pv.sum(axis=0)
    pv = pv.reset_index().rename(columns={"index": index_col})
    pv.columns.name = None
    return pv


pivots = {
    "Worksite-wise Headcount": pivot_with_totals(
        headcount_by("LegalCo", "Worksite"), "LegalCo", "Worksite", ENTITY_ORDER, LOCATION_ORDER),
    "Employment Type-wise Headcount": pivot_with_totals(
        headcount_by("LegalCo", "EmpType"), "LegalCo", "EmpType", ENTITY_ORDER),
    "Grade-wise Headcount": pivot_with_totals(
        headcount_by("LegalCo", "Grade"), "LegalCo", "Grade", ENTITY_ORDER, DESIGNATION_ORDER),
    "Business Unit-wise Headcount": pivot_with_totals(
        headcount_by("BizUnit", "LegalCo"), "BizUnit", "LegalCo", None, ENTITY_ORDER),
}


# ── CELL 6: HTML ───────────────────────────────────────────────────────────
ACCENT = "#2E4057"
TOTAL_BG = "#E3E8EF"


def html_table(df, highlight_last=True):
    """Email-safe inline-styled table; last row/column bolded as totals."""
    th = (f'style="background:{ACCENT};color:#fff;font-weight:bold;padding:6px;'
          f'white-space:nowrap;"')
    out = ['<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;'
           'width:100%;font-size:13px;text-align:center;font-family:Arial;">',
           "<thead><tr>", *[f"<th {th}>{c}</th>" for c in df.columns], "</tr></thead><tbody>"]
    last_r, last_c = len(df) - 1, len(df.columns) - 1
    for ri, (_, row) in enumerate(df.iterrows()):
        out.append("<tr>")
        for ci, v in enumerate(row):
            bold = highlight_last and (ri == last_r or ci == last_c)
            style = f"background:{TOTAL_BG};font-weight:bold;" if bold else ""
            out.append(f'<td style="{style}padding:6px;white-space:nowrap;">{v}</td>')
        out.append("</tr>")
    out.append("</tbody></table><br>")
    return "".join(out)


def people_section(title, df):
    if df is None or df.empty:
        return f"<h3>{title}</h3><p>None.</p>"
    html = f"<h3>{title}</h3><p>Total: <b>{len(df)}</b></p>"
    if len(df) <= MAX_LISTED_EMPLOYEES:
        html += html_table(df, highlight_last=False)
    return html


if first_run:
    changes_html = "<p><b>Note:</b> first run - change comparison starts from the next report.</p>"
else:
    changes_html = (
        "<h2>Headcount Changes Since Last Report</h2>"
        "<h3>Company-wise</h3>" + html_table(comparison["entity"])
        + "<h3>Business Unit-wise</h3>"
        + (html_table(comparison["dept"].head(10), highlight_last=False)
           if not comparison["dept"].empty else "<p>No department-level changes.</p>")
        + people_section("New Employees", comparison["added"])
        + people_section("Exited Employees", comparison["removed"])
    )

pivot_html = "".join(f"<h2>{title}</h2>{html_table(df)}" for title, df in pivots.items())

email_body = f"""
<html><body style="font-family:Segoe UI,Arial,sans-serif;font-size:15px;color:#333;">
<p>Dear Team,</p>
<p>Please find the consolidated headcount summary below.</p>
{changes_html}
{pivot_html}
<p>Regards,<br><b>{SIGN_OFF}</b></p>
<p style="font-size:12px;color:#777;"><i>Auto-generated from Microsoft Fabric.</i></p>
</body></html>
"""


# ── CELL 7: Send, then persist snapshot ────────────────────────────────────
msg = MIMEMultipart("alternative")
msg["From"] = SMTP_USER
msg["To"] = ", ".join(EMAIL_TO)
if MAIL_CC:
    msg["Cc"] = ", ".join(MAIL_CC)
msg["Subject"] = "Employee Headcount Summary" + (" - First Run" if first_run else " - With Change Analysis")
msg.attach(MIMEText(email_body, "html"))

try:
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg, from_addr=SMTP_USER, to_addrs=EMAIL_TO + MAIL_CC)
    print("Email sent.")

    # Snapshot is only overwritten after a successful send, so a failed run
    # doesn't silently "consume" the changes nobody was told about.
    (combined_df.select(*DETAIL_COLS)
        .withColumn("captured_at", F.current_timestamp())
        .write.mode("overwrite").option("overwriteSchema", "true")
        .format("delta").saveAsTable(SNAPSHOT_TABLE))
    print(f"Snapshot saved to {SNAPSHOT_TABLE}")
except Exception as exc:
    print(f"Send failed - snapshot NOT updated: {exc}")
    raise
