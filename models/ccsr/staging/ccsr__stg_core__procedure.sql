
select
      encounter_id
    , claim_id
    , person_id
    , normalized_code
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__procedure') }}
