{{ config(materialized='table') }}

with products as (
    select
        pi.prd_id as product_id,
        pi.prd_key as product_number,
        pi.prd_name as product_name,
        pi.cat_id as category_id,
        pc.cat as category,
        pc.subcat as subcategory,
        pc.maintenance,
        pi.prd_cost as product_cost,
        pi.prd_line as product_line,
        pi.prd_start_dt as start_date
    from {{ ref('stg_crm_prd_info') }} as pi
    left join {{ ref('stg_erp_px_cat_g1v2') }} as pc
        on pi.cat_id = pc.id
    where pi.prd_end_dt is null
)

select
    row_number() over (order by start_date, product_id) as product_key,
    product_id,
    product_number,
    product_name,
    category_id,
    category,
    subcategory,
    maintenance,
    product_cost,
    product_line,
    start_date
from products
