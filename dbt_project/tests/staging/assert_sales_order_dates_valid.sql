-- Check that order date is not after shipping or due date
select *
from {{ ref('stg_crm_sales_details') }}
where sls_order_dt > sls_ship_dt
   or sls_order_dt > sls_due_dt
