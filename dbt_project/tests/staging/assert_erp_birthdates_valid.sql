-- Check that birthdates are within a reasonable range (1924 to today)
select distinct bdate
from {{ ref('stg_erp_cust_az12') }}
where bdate < '1924-01-01'
   or bdate > current_date
