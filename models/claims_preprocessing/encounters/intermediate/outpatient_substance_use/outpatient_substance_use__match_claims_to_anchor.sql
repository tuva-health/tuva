
select distinct m.patient_id
 , m.start_date
 , m.claim_id
 , m.claim_line_number
 , u.old_encounter_id
from {{ ref('encounters__stg_medical_claim') }} m
inner join {{ ref('outpatient_substance_use__generate_encounter_id') }} u on m.patient_id = u.patient_id
and
m.start_date = u.start_date
