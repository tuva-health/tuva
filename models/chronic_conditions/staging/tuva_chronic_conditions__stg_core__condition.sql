
select
      person_id
    , normalized_code
    , recorded_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__condition') }}
