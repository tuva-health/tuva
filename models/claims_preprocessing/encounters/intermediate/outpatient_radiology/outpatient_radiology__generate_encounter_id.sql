
select patient_id
,start_date
,hcpcs_code
,dense_rank() over (order by patient_id, start_date, hcpcs_code) as old_encounter_id
from {{ ref('outpatient_radiology__anchor_events') }}