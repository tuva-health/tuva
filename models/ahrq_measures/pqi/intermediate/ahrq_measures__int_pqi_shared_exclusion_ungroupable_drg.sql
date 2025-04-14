
-- Exclude encounters with ungroupable DRG
select
    encounter_id
  , data_source
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }}
where
  drg_code = '999'
