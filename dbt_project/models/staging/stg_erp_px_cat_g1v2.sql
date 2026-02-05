select
    id,
    cat,
    subcat,
    case
        when
            upper(
                trim(regexp_replace(maintenance, E'[\r\n]', '', 'g'))
            ) = 'YES'
            then 'Yes'
        when
            upper(
                trim(regexp_replace(maintenance, E'[\r\n]', '', 'g'))
            ) = 'NO'
            then 'No'
        else maintenance
    end as maintenance,
    current_timestamp as dws_create_date
from {{ source('bronze', 'erp_px_cat_g1v2') }}
