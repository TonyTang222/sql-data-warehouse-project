-- Inflate bronze.crm_sales_details from 60K to ~10M rows
-- by cross-joining with generate_series and mutating sls_ord_num to keep keys unique.
--
-- Why CROSS JOIN generate_series instead of seed:
--   - dbt seed re-inserts CSVs row-by-row via psycopg2 — ~15-30 min for 10M rows
--   - INSERT INTO ... SELECT inside the DB stays in memory/disk — ~10-30 sec for 10M rows
--
-- Why mutate sls_ord_num:
--   - fact_sales unique_key = (order_number, product_key)
--   - if we copy the same SO75123 167 times, MERGE would dedupe and final table stays at 60K
--   - suffixing with the multiplier (_v1, _v2, ...) keeps each copy a unique fact row

\timing on

-- Backup original (only if not already backed up)
CREATE TABLE IF NOT EXISTS bronze._crm_sales_details_original AS
SELECT * FROM bronze.crm_sales_details;

-- Reset to original size before scaling (idempotent)
TRUNCATE bronze.crm_sales_details;
INSERT INTO bronze.crm_sales_details SELECT * FROM bronze._crm_sales_details_original;

-- Inflate to 10M rows (60K × 167 copies)
INSERT INTO bronze.crm_sales_details (
    sls_ord_num, sls_prd_key, sls_cust_id,
    sls_order_dt, sls_ship_dt, sls_due_dt,
    sls_sales, sls_quantity, sls_price
)
SELECT
    orig.sls_ord_num || '_v' || g.n   AS sls_ord_num,   -- e.g., SO75123_v2
    orig.sls_prd_key,
    orig.sls_cust_id,
    orig.sls_order_dt,
    orig.sls_ship_dt,
    orig.sls_due_dt,
    orig.sls_sales,
    orig.sls_quantity,
    orig.sls_price
FROM bronze._crm_sales_details_original AS orig
CROSS JOIN generate_series(1, 166) AS g(n);    -- 166 extra copies + 1 original = 167x ≈ 10M

-- Verify
SELECT count(*) AS total_rows FROM bronze.crm_sales_details;
SELECT pg_size_pretty(pg_total_relation_size('bronze.crm_sales_details')) AS table_size;
