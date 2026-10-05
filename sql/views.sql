-- Reporting views on top of the star schema (portable SQL, no dialect date functions).

DROP VIEW IF EXISTS vw_sales_flat;
CREATE VIEW vw_sales_flat AS
SELECT f.transaction_id, d.full_date, d.year_num, d.quarter_label, d.year_month,
       a.area_name, a.community, a.zone, p.property_type, p.rooms, p.sale_stage,
       b.buyer_type, t.txn_type,
       f.transaction_value, f.area_sqm, f.price_per_sqm
FROM fact_transactions f
JOIN dim_date d             ON d.date_key = f.date_key
JOIN dim_area a             ON a.area_key = f.area_key
JOIN dim_property p         ON p.property_key = f.property_key
JOIN dim_buyer_type b       ON b.buyer_type_key = f.buyer_type_key
JOIN dim_transaction_type t ON t.txn_type_key = f.txn_type_key;

DROP VIEW IF EXISTS vw_monthly_market;
CREATE VIEW vw_monthly_market AS
SELECT d.year_month, d.year_num, d.month_num,
       COUNT(*)                          AS txn_count,
       SUM(f.transaction_value)          AS total_value,
       AVG(f.price_per_sqm)              AS avg_price_per_sqm
FROM fact_transactions f
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_transaction_type t ON t.txn_type_key = f.txn_type_key
WHERE t.txn_type = 'Sales'
GROUP BY d.year_month, d.year_num, d.month_num;

DROP VIEW IF EXISTS vw_area_performance;
CREATE VIEW vw_area_performance AS
SELECT a.area_name, a.zone, d.year_num,
       COUNT(*)                 AS txn_count,
       SUM(f.transaction_value) AS total_value,
       AVG(f.price_per_sqm)     AS avg_price_per_sqm
FROM fact_transactions f
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_area a ON a.area_key = f.area_key
JOIN dim_transaction_type t ON t.txn_type_key = f.txn_type_key
WHERE t.txn_type = 'Sales'
GROUP BY a.area_name, a.zone, d.year_num;

DROP VIEW IF EXISTS vw_dq_latest;
CREATE VIEW vw_dq_latest AS
SELECT * FROM dq_log
WHERE run_id = (SELECT run_id FROM etl_run_log ORDER BY started_at DESC LIMIT 1);
