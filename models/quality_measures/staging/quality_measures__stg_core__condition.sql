select
      person_id
    , claim_id
    , encounter_id
    , recorded_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__condition') }}
