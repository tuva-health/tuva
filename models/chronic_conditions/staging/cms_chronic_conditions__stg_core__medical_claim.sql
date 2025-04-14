
select
      claim_id
    , person_id
    , claim_start_date
    , drg_code_type
    , drg_code
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__medical_claim') }}
