-- Check for unwanted leading/trailing spaces in customer key
select cst_key
from {{ ref('stg_crm_cust_info') }}
where cst_key != trim(cst_key)
