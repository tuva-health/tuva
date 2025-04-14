
select
      claim_id
    , person_id
    , recorded_date
    , normalized_code_type
    , normalized_code
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__condition') }}
where claim_id is not null
