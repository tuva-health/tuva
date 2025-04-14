
select
    encounter_id
    , claim_id
    , person_id
    , normalized_code
    , condition_rank
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__condition') }}
where normalized_code_type = 'icd-10-cm'
