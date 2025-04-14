
select
    person_id
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__patient') }}
