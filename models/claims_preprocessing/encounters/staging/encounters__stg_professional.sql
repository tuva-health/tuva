
select distinct
  claim_id
, claim_line_number
, claim_line_id
, service_type
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_professional') }} as a
