select 
    discharge_status_code
from {{ ref('stg_encounter') }}
where encounter_type = 'acute inpatient'
    and discharge_status_code is null