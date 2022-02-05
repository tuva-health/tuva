select 
    admit_type_code
from {{ ref('stg_encounter') }}
where encounter_type = 'acute inpatient'
    and admit_type_code is null