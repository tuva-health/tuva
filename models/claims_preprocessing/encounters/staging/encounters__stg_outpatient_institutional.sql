

select distinct
  claim_id
, service_type
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_outpatient_institutional') }} as a
