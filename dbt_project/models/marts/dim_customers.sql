{{ config(materialized='table') }}

with customers as (
    select
        ci.cst_id as customer_id,
        ci.cst_key as customer_number,
        ci.cst_firstname as first_name,
        ci.cst_lastname as last_name,
        ca.bdate as birthdate,
        la.cntry as country,
        ci.cst_marital_status as marital_status,
        ci.cst_create_date as create_date,
        case
            when ci.cst_gndr != 'N/A' then ci.cst_gndr
            else coalesce(ca.gen, 'N/A')
        end as gender
    from {{ ref('stg_crm_cust_info') }} as ci
    left join {{ ref('stg_erp_cust_az12') }} as ca
        on ci.cst_key = ca.cid
    left join {{ ref('stg_erp_loc_a101') }} as la
        on ci.cst_key = la.cid
)

select
    row_number() over (order by customer_id) as customer_key,
    customer_id,
    customer_number,
    first_name,
    last_name,
    birthdate,
    country,
    marital_status,
    create_date,
    gender
from customers
