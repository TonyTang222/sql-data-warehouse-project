-- Check for unwanted spaces in category fields
select *
from {{ ref('stg_erp_px_cat_g1v2') }}
where cat != trim(cat)
   or subcat != trim(subcat)
   or maintenance != trim(maintenance)
