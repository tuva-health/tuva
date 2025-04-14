select
      person_id
    , sex
    , birth_date
    , death_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__patient') }}
