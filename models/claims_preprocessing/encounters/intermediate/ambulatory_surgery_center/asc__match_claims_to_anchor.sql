
select dat.old_encounter_id
,dat.encounter_start_date
,dat.encounter_end_date
,med.claim_id
,med.claim_line_number

,row_number () over (partition by med.claim_id, claim_line_number order by dat.old_encounter_id) as claim_attribution_number
from {{ ref('encounters__stg_medical_claim') }} med
inner join {{ ref('asc__start_end_dates') }} dat on med.patient_id = dat.patient_id
and med.start_date between dat.encounter_start_date and dat.encounter_end_date
