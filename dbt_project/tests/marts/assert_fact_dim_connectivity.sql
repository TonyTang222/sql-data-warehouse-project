-- Check referential integrity: all fact records should have valid dimension keys
select *
from {{ ref('fact_sales') }} f
left join {{ ref('dim_customers') }} c
    on c.customer_key = f.customer_key
left join {{ ref('dim_products') }} p
    on p.product_key = f.product_key
where p.product_key is null
   or c.customer_key is null
