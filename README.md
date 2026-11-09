# Retail Analytics Notebooks (Microsoft Fabric / PySpark)

Four production-style reporting notebooks for a multi-brand, multi-store retail
group. Each one reads from a Fabric Lakehouse, applies business logic in
PySpark/pandas, and emails a formatted report (Excel or HTML) on a schedule.

> All brands, stores, tables, columns, terminal IDs and e-mail addresses are
> **fictional**. The logic mirrors real reporting pipelines; the data model is
> illustrative.

| # | Notebook | Output | What it shows |
|---|----------|--------|---------------|
| 01 | `store_payment_reconciliation` | Excel (4 sheets) | Per store, per day: POS card/UPI vs card-terminal + UPI-aggregator collections, cash register deposits, implied cash per order |
| 02 | `hr_headcount_change_report` | HTML email | Multi-entity headcount pivots + changes since last run (added / exited employees, department deltas) |
| 03 | `weekly_brand_performance` | Excel dashboard | Brand-wise week & MTD sales vs target, GRN, returns, payments, stock value, margin, stock cover |
| 04 | `daily_sales_snapshot` | HTML email | Store-wise daily sale, MTD vs target, shortfall and run-rate needed for the rest of the week |

## Techniques demonstrated

**Reconciliation**
- Grouping by *(order, date)* on both the sale-event and payment sides so
  multi-day orders (advance + balance, edits, returns) land on the right day.
- Implied cash per order-day = sale amount − non-cash payments, with an
  advance/balance stage label derived from event ordering (window functions).
- Sign correction for coupon reversals that arrive as positive amounts.
- Many-to-one terminal → store mapping; two collection sources combined for UPI.
- Configurable consolidated window: month-to-date, rolling N days, or rerun a past month.

**Data quality**
- Harmonising schemas across entity tables (typos, spaces, punctuation in headers).
- Fixing day/month swapped dates that arrive pre-parsed from spreadsheets.
- Cleaning currency strings, trimming keys, de-duplicating on composite keys.
- A freshness gate that stops the run if upstream loads haven't landed.

**Business logic**
- Carving a sub-brand out of one brand's online sales by a maker-name regex and
  attributing it to another brand.
- Avoiding double counting when a legacy POS already includes online orders.
- Unpivoting wide target sheets with `stack()`; combining brand + sub-brand targets.
- Shortfall spread across the remaining days of the week.
- Snapshot-diff change tracking where the snapshot is only overwritten after a
  successful send.

**Output**
- Styled `openpyxl` workbooks (conditional fills on differences, frozen panes, filters).
- Email-safe inline-styled HTML tables with totals rows/columns.

## Repo layout

```
notebooks/
  01_store_payment_reconciliation.py
  02_hr_headcount_change_report.py
  03_weekly_brand_performance.py
  04_daily_sales_snapshot.py
sample_data/
  generate_sample_data.py     # builds fake tables so you can try 01 locally
.env.example
```

The `.py` files use `# ── CELL n` markers; paste each block into a Fabric (or
Databricks / Synapse) notebook cell, or import the file directly.

## Running

1. Copy `.env.example` to `.env` (or set the same keys as notebook environment
   variables / Key Vault secrets). **Never commit real credentials.**
2. Point the table constants at the top of each notebook to your lakehouse.
3. To try notebook 01 without real data:

   ```bash
   pip install pandas
   python sample_data/generate_sample_data.py --days 30
   ```

   This writes fictional CSVs to `sample_data/csv/`. Load them into a Fabric
   Lakehouse as tables (the script's docstring lists the target names), then run
   the notebook with `SEND_EMAIL = False` and inspect `excel_bytes`.

## Requirements

Python 3.10+, PySpark 3.4+, pandas, openpyxl, pytz.
