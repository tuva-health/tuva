-- Exclude transfers from hospital, SNF, or other healthcare facility
select 
    encounter_id
  , data_source
from {{ ref('quality_measures__stg_pqi_inpatient_encounter') }}
where 
  admit_source_code in ('4', '5', '6')
