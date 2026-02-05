with most_recent_cus as (
    select
        *,
        row_number() over (
            partition by cst_id
            order by cst_create_date desc
        ) as rank_flag
    from {{ source('bronze', 'crm_cust_info') }}
    where cst_id is not null
)

select
    cst_id,
    cst_key,
    cst_create_date,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,
    case
        when upper(trim(cst_marital_status)) = 'S' then 'Single'
        when upper(trim(cst_marital_status)) = 'M' then 'Married'
        else 'N/A'
    end as cst_marital_status,
    case
        when upper(trim(cst_gndr)) = 'M' then 'Male'
        when upper(trim(cst_gndr)) = 'F' then 'Female'
        else 'N/A'
    end as cst_gndr,
    current_timestamp as dws_create_date
from most_recent_cus
where rank_flag = 1
