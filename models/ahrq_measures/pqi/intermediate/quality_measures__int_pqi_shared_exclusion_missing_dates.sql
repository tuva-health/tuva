-- Exclude encounters with missing start dates
select
    encounter_id
  , data_source
from {{ ref('quality_measures__stg_pqi_inpatient_encounter') }}
where 
  encounter_start_date is null
