-- Exclude encounters with ungroupable DRG
select 
    encounter_id
  , data_source
from {{ ref('ahrq_measures__stg_pqi_inpatient_encounter') }}
where 
  ms_drg_code = '999'
