select
    prd_id,
    prd_start_dt,
    prd_nm as prd_name,
    (
        lead(prd_start_dt) over (
            partition by substring(prd_key from 7)
            order by prd_start_dt
        ) - interval '1 day'
    )::date as prd_end_dt,
    replace(substring(prd_key from 1 for 5), '-', '_') as cat_id,
    substring(prd_key from 7) as prd_key,
    coalesce(prd_cost, 0) as prd_cost,
    case
        when upper(trim(prd_line)) = 'M' then 'Mountain'
        when upper(trim(prd_line)) = 'R' then 'Road'
        when upper(trim(prd_line)) = 'S' then 'Other sales'
        when upper(trim(prd_line)) = 'T' then 'Touring'
        else 'N/A'
    end as prd_line,
    current_timestamp as dws_create_date
from {{ source('bronze', 'crm_prd_info') }}
