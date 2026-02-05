-- Check for invalid date ranges where start date is after end date
select *
from {{ ref('stg_crm_prd_info') }}
where prd_end_dt < prd_start_dt
