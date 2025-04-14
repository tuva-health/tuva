
select
      claim_id
    , person_id
    , paid_date
    , ndc_code
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__pharmacy_claim') }}
