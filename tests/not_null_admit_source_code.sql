select 
    admit_source_code
from {{ ref('stg_encounter') }}
where encounter_type = 'acute inpatient'
    and admit_source_code is null