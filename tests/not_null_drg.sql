select 
    drg
from {{ ref('stg_encounter') }}
where encounter_type = 'acute inpatient'
    and drg is null