# Project Description: Dubai Real Estate Data Warehouse

> **Before you use this:** the numbers below come from running this exact pipeline on the included synthetic dataset (structured like Dubai Land Department transaction exports, with deliberately injected defects). Run `./run_all.sh` yourself and confirm them, and if you switch to real DLD data, replace them with your own. Only claim what you have built and can demo.

---

## 1. One-line summary

Designed and built an end-to-end data warehouse for Dubai property transactions: a Python/SQL ETL pipeline loading a Kimball star schema (with SCD Type 2 history, incremental loads and automated data quality checks) feeding a five-page Power BI report.

## 2. CV bullets (pick 3 to 4)

- Designed a Kimball **star schema** (1 fact, 5 conformed dimensions) for ~280K property transactions, with surrogate keys, a date dimension and a degenerate transaction ID.
- Built a **Python (pandas, SQLAlchemy) ETL pipeline** covering staging, cleansing, deduplication, reject logging and **incremental, idempotent loading**; re-running a batch loads zero duplicate rows.
- Implemented **SCD Type 2** on the area dimension with hash-based change detection and point-in-time key lookups, so historical sales stay attributed to the zone valid on the sale date.
- Added **11 automated data quality checks** per run (row reconciliation, referential integrity, null and uniqueness checks, SCD2 integrity, range and reject-rate thresholds) logged to audit tables.
- Modelled the warehouse in **Power BI** with 20+ DAX measures (YoY, YTD, MoM, 12-month rolling average, market share, ranking), row-level security by zone, and a pipeline-health page for monitoring.

## 3. LinkedIn / portfolio description

I built an end-to-end analytics pipeline for Dubai's property market. Raw transaction files are extracted and staged, cleaned and deduplicated in Python, then loaded into a dimensional star schema in SQL, with Type 2 history on areas that get re-zoned and an incremental load that is safe to re-run. Every load runs 11 automated data quality checks and writes to audit tables. On top sits a Power BI model with DAX time intelligence, drill-down by area and buyer type, row-level security, and a monitoring page that shows pipeline health to business users.

Stack: Python, pandas, SQLAlchemy, SQL (SQLite / MySQL / PostgreSQL / SQL Server compatible), Power BI, DAX, Kimball dimensional modelling.

## 4. Full project description (for the portfolio site or README summary)

### Problem
Property transaction data arrives as flat files with duplicates, missing values, invalid areas, inconsistent casing and stray whitespace, and the reference data (area zoning) changes over time. Analysts need trustworthy, fast, repeatable reporting on prices, volumes and buyer behaviour.

### What I did

**1. Source data and profiling.** Generated a dataset modelled on the Dubai Land Department transaction export (250K historical rows for 2020 to 2025 and 30K incremental rows for 2026), including an area master that is re-zoned in a second version. Defects were injected on purpose (about 1% each of missing values, zero areas, negative values, casing and whitespace issues, and duplicates) so the cleansing logic has real work to do.

**2. Dimensional design.** Chose the grain of one row per transaction and designed a star schema: `fact_transactions` with five dimensions (`dim_date`, `dim_area`, `dim_property`, `dim_buyer_type`, `dim_transaction_type`). Used surrogate keys throughout, kept the transaction ID as a degenerate dimension, and added indexes on every fact foreign key.

**3. ETL pipeline.** Wrote a modular Python pipeline: extract, load to staging, standardise and validate, load dimensions, load facts, run quality checks, refresh reporting views and export. Rejected rows go to a `reject_transactions` table with a reason code instead of being silently dropped.

**4. SCD Type 2.** Implemented history tracking on `dim_area`. An MD5 hash of the tracked attributes detects changes; the old row is closed (`valid_to`, `is_current = 0`) and a new version is inserted. When loading facts, each sale is joined to the area version valid on its transaction date, so 2025 sales stay under the old zone and 2026 sales use the new one.

**5. Incremental loading.** The fact load anti-joins on `transaction_id`, so only new rows are inserted and a full re-run of the same batch loads 0 rows. A high-water mark is stored in `etl_state`.

**6. Data quality and observability.** Every run executes 11 checks (row-count reconciliation, foreign-key integrity on all five dimensions, null measures, business-key uniqueness, SCD2 integrity, price-per-sqm plausibility, reject-rate threshold) and writes results to `dq_log` and run statistics to `etl_run_log`.

**7. Power BI.** Imported the star schema, defined one-to-many single-direction relationships, marked the date table, and wrote 20+ DAX measures. Built five pages: Executive Overview, Area Drill-down, Trends and Buyer Behaviour, SCD2 History Demo, and Pipeline Health. Added row-level security by zone.

### Results (from the included dataset; verify on your own run)

| Metric | Value |
|---|---|
| Raw rows processed (historical + incremental) | 282,800 |
| Rows loaded to the fact table | 271,714 |
| Rows rejected, with reason codes | 8,286 (2.9%) |
| Duplicate rows removed | 2,800 |
| Dimension tables / fact tables | 5 / 1 |
| Automated data quality checks per run | 11 (all PASS on both loads) |
| Historical load time (about 252K rows) | ~9 seconds |
| Incremental load time (about 30K rows) | ~3 seconds |
| Re-run of an already-loaded batch | 0 new rows loaded |
| Areas versioned by SCD2 | 3 (history preserved) |

> Timings are from a sandbox run on SQLite; your numbers will differ. If you add a performance claim (for example "indexing cut a query from X s to Y s"), measure it first with `EXPLAIN` before and after the index.

### Business questions the report answers
- Which areas and zones drive the most sales value, and how is that changing year over year?
- How do prices per sqm trend after smoothing out seasonality?
- What share of the market is off-plan, investor-led or mortgage-financed?
- How would the market look under the old versus the new zone classification?

## 5. Interview talking points

**Why a star schema?** It keeps queries simple and fast for BI tools, gives Power BI a clean one-to-many model, and separates descriptive context (dimensions) from measurable events (the fact).

**Why SCD Type 2 on areas and not on everything?** Only attributes whose history matters for reporting need versioning. Property type, buyer type and transaction type are stable, so Type 1 / insert-only is enough and keeps the model simple.

**How does your pipeline handle bad data?** Standardise what is fixable (casing, whitespace, date format), drop exact duplicates by business key, and send everything else to a reject table with a reason, so nothing disappears silently and the reject rate is monitored.

**How do you make loads safe to re-run?** The fact load is idempotent: it anti-joins on the business key before inserting, so a retry after a failure cannot create duplicates.

**Why is average price per sqm computed as total value / total area?** Averaging row-level prices gives small units the same weight as large villas. The weighted version reflects the actual market.

**What would you change for production?**
- Move the database to PostgreSQL / SQL Server / a cloud warehouse (Snowflake, BigQuery, Synapse).
- Orchestrate with Airflow or Azure Data Factory, with alerting on failed DQ checks.
- Land raw files in object storage and keep them immutable.
- Add partitioning on `date_key` and a watermark-based extract instead of file batches.
- Add unit tests for the transformation functions (CI with GitHub Actions) and dbt for the SQL layer.
- Map RLS roles to Azure AD groups and use an on-premises gateway or cloud source for scheduled refresh.

**What was hardest?** Getting the SCD2 point-in-time join right: the fact load has to match each sale to the dimension version valid on its date, otherwise facts silently attach to the wrong zone.

## 6. Suggested screenshots for the portfolio

1. ER diagram (render the Mermaid block in the README)
2. Terminal output of `run_all.sh` showing the DQ checks
3. Power BI: Executive Overview and Area Drill-down
4. Power BI: SCD2 History Demo and Pipeline Health
