-- Exclude encounters with missing start dates
select
    encounter_id
  , data_source
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }}
where
  encounter_start_date is null
