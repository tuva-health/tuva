select patient_id
,start_date
,dense_rank() over (order by patient_id, start_date) as old_encounter_id
from {{ ref('outpatient_injections__anchor_events') }} 