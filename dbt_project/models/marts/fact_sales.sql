{{ config(
    materialized='incremental',
    unique_key=['order_number', 'product_key'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    post_hook=[
        "create index if not exists idx_{{ this.identifier }}_order_date on {{ this }} (order_date)",
        "create index if not exists idx_{{ this.identifier }}_customer_key on {{ this }} (customer_key)",
        "create index if not exists idx_{{ this.identifier }}_product_key on {{ this }} (product_key)"
    ]
) }}

select
    sd.sls_ord_num as order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price as price
from {{ ref('stg_crm_sales_details') }} as sd
left join {{ ref('dim_customers') }} as cu
    on sd.sls_cust_id = cu.customer_id
left join {{ ref('dim_products') }} as pr
    on sd.sls_prd_key = pr.product_number

{% if is_incremental() %}
    -- Watermark: only pick up rows newer than what's already in target.
    -- On the very first run target doesn't exist, so this block is skipped entirely.
    where sd.sls_order_dt > (
        select coalesce(max(t.order_date), '1900-01-01') from {{ this }} as t
    )
{% endif %}
