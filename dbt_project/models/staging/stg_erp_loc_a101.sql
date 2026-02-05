select
    replace(cid, '-', '') as cid,
    case
        when trim(regexp_replace(cntry, E'[\r\n]', '', 'g')) = 'DE'
            then 'Germany'
        when trim(regexp_replace(cntry, E'[\r\n]', '', 'g')) in ('US', 'USA')
            then 'United States'
        when
            trim(regexp_replace(cntry, E'[\r\n]', '', 'g')) = ''
            or cntry is null
            then 'N/A'
        else trim(regexp_replace(cntry, E'[\r\n]', '', 'g'))
    end as cntry,
    current_timestamp as dws_create_date
from {{ source('bronze', 'erp_loc_a101') }}
