
/* Exclude patients with missing age */
select
    data_source
    , person_id
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_patient') }}
where birth_date is null
