select
    case
        when cid like 'NAS%' then substring(cid from 4)
        else cid
    end as cid,
    case
        when bdate > current_date then null
        when bdate < '1924-01-01' then null
        else bdate
    end as bdate,
    case
        when upper(replace(trim(gen), E'\r', '')) in ('M', 'MALE')
            then 'Male'
        when upper(replace(trim(gen), E'\r', '')) in ('F', 'FEMALE')
            then 'Female'
        else 'N/A'
    end as gen,
    current_timestamp as dws_create_date
from {{ source('bronze', 'erp_cust_az12') }}
