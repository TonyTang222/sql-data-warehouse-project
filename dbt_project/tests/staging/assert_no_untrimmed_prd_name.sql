-- Check for unwanted leading/trailing spaces in product name
select prd_name
from {{ ref('stg_crm_prd_info') }}
where prd_name != trim(prd_name)
