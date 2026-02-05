-- Check for null or negative product costs
select prd_cost
from {{ ref('stg_crm_prd_info') }}
where prd_cost < 0 or prd_cost is null
