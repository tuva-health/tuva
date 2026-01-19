-- Flattening months to 1 person per year
select distinct 
  person_id
  , {{ date_part('year', 'collection_end_date') }} as collection_year
  , payer
from {{ ref('cms_hcc__int_members') }}
-- Don't support ESRD risk scores yet
where enrollment_status != 'ESRD'