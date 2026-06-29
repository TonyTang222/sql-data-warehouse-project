{{ config(materialized='table') }}

-- Time spine for MetricFlow. One row per calendar day from 2010 (covers
-- the earliest order_date in fact_sales) through 2030 (a generous buffer
-- for any forecasting use case). MetricFlow LEFT JOINs this against
-- measures so days with zero activity still appear in time-series output
-- instead of dropping out.
select cast(d as date) as date_day
from generate_series(
    date '2010-01-01',
    date '2030-12-31',
    interval '1 day'
) as d
