# Power BI Build Guide

Follow this top to bottom and you will have the finished report.

## 1. Connect

**Option A (quickest): CSV import.** After running the ETL, open Power BI Desktop, then
`Get data > Folder` (or `Text/CSV` per file) and load everything in `output/powerbi_export/`:
`fact_transactions, dim_date, dim_area, dim_property, dim_buyer_type, dim_transaction_type, dq_log, etl_run_log`.

**Option B (production style): connect to a real database.** Run the ETL with
`--db "mysql+pymysql://user:pw@host/dubai_dw"` (or PostgreSQL / SQL Server), then
`Get data > MySQL / PostgreSQL / SQL Server`, and pick the same tables.
Use **Import** mode for this data size (~270k fact rows); use DirectQuery only if the table grows into the hundreds of millions.

## 2. Power Query clean-up (Transform data)

- Set types: `full_date`, `valid_from`, `valid_to` -> Date; all `*_key` and `*_id` -> Whole number; `transaction_value`, `price_per_sqm`, `area_sqm` -> Fixed decimal number.
- Remove the audit column `_batch_id` from `fact_transactions` (not needed in the model).
- Rename tables to friendly names only if you want; the DAX file assumes the original names.

## 3. Model (Model view)

Star schema, all relationships **one-to-many, single direction, dimension -> fact**:

| From (one) | To (many) |
|---|---|
| `dim_date[date_key]` | `fact_transactions[date_key]` |
| `dim_area[area_key]` | `fact_transactions[area_key]` |
| `dim_property[property_key]` | `fact_transactions[property_key]` |
| `dim_buyer_type[buyer_type_key]` | `fact_transactions[buyer_type_key]` |
| `dim_transaction_type[txn_type_key]` | `fact_transactions[txn_type_key]` |

- `dq_log` and `etl_run_log` stay **unrelated** (they are standalone audit tables).
- Right-click `dim_date` > **Mark as date table** > `full_date`.
- Sort `month_name` by `month_num`, and `day_name` by `day_of_week`.
- Hide all `*_key` columns and the raw numeric columns in the fact (use measures instead).
- Hide `row_hash`, `valid_from`, `valid_to` in `dim_area` (keep `is_current` visible).

## 4. Measures

Create an empty table (`Enter data`) named `_Measures`, then paste each measure from `measures.dax`. Format:
- Value measures -> `#,0` with a currency prefix `AED`
- `%` measures -> percentage, 1 decimal
- `Avg Price per sqm` -> `#,0`

## 5. Report pages

**Page 1: Executive Overview**
- KPI cards: Total Sales Value, Total Transactions, Avg Price per sqm, Sales Value YoY % (with the `YoY Arrow` measure)
- Line chart: monthly Sales Value with `Sales Value PY` as a second line
- Column chart: transactions by quarter
- Donut: share by property type
- Slicers: Year, Quarter, Transaction Type (leave the Sales filter in the measures; use the slicer for Mortgage/Gifts exploration)

**Page 2: Area Drill-down**
- Bar chart: top areas by Sales Value (use `Area Rank (by Value)` as a Top N filter)
- Matrix: Area x Year with Value, Transactions, Avg Price per sqm, conditional-format data bars
- Scatter: x = Avg Price per sqm, y = Total Transactions, size = Total Sales Value, legend = Zone
- Tooltip page: mini trend per area

**Page 3: Trends and Buyer Behaviour**
- Line chart: `Price per sqm 12M Rolling Avg`
- Stacked column: Off-Plan vs Ready by month
- 100% stacked bar: buyer type mix by area
- Cards: `Off-Plan Share %`, `Investor Share %`, `Mortgage Penetration %`
- Decomposition tree: Sales Value by Zone > Area > Property Type

**Page 4: SCD2 History Demo** (a nice talking point)
- Table: `dim_area[area_name]`, `zone`, `valid_from`, `valid_to`, `is_current`, filtered to the three re-zoned areas
- Clustered column: Sales Value by Zone and Year, showing how the same area's sales move to a new zone after 2026-01-01 while 2025 history stays under the old zone

**Page 5: Pipeline Health** (shows engineering maturity)
- Table: `etl_run_log` (rows extracted / rejected / duplicates / loaded / duration)
- Table: `dq_log` with conditional colour on `status` (PASS green, WARN amber, FAIL red)
- Cards: `DQ Checks Failed`, `Last Refresh Status`

## 6. Row-level security (bonus)

`Modeling > Manage roles`: create a role `Zone Manager` with a table filter on `dim_area`:

```
[zone] = "Waterfront"
```

Use `View as` to test. Mention in the write-up that production would map roles to Azure AD groups.

## 7. Publish and refresh

- Publish to Power BI Service, then configure a **scheduled refresh** (needs an on-premises data gateway if the DB is local).
- Schedule the ETL (cron, Windows Task Scheduler, or Airflow) to run before the dataset refresh.

## 8. Screenshots for the portfolio

Capture each page (1280x720), blur nothing (the data is synthetic or public), and add them to `docs/` and the README.
