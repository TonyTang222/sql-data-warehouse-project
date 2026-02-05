-- Check data consistency: sales_amount should equal quantity * price
select distinct
    sls_sales,
    sls_quantity,
    sls_price
from {{ ref('stg_crm_sales_details') }}
where sls_sales != sls_quantity * sls_price
   or sls_sales is null
   or sls_quantity is null
   or sls_price is null
   or sls_sales <= 0
   or sls_quantity <= 0
   or sls_price <= 0
