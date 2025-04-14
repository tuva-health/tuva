
-- Exclude transfers from hospital, SNF, or other healthcare facility
select
    encounter_id
  , data_source
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }}
where
  admit_source_code in ('4', '5', '6')
