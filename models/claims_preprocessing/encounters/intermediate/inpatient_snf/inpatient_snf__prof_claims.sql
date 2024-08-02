with first_claim as (
    select *
    from {{ ref('inpatient_snf__generate_encounter_id') }}
    where encounter_claim_number = 1
)

,join_first_claim_dates as (
    select f.*
    ,dat.encounter_end_date
    ,dat.encounter_start_date
    from first_claim f 
    inner join {{ ref('inpatient_snf__start_end_dates') }} dat on f.encounter_id = dat.encounter_id
)

-- ensuring each prof claim is only attributed to one institutional claim with claim_attribution_number
select dat.encounter_id
,dat.encounter_start_date
,dat.encounter_end_date
,prof.claim_id
,prof.claim_line_number
-- ,med.start_date
-- ,med.end_date
-- ,med.place_of_service_code
-- ,med.place_of_service_description
,row_number () over (partition by prof.claim_line_id order by dat.encounter_id) as claim_attribution_number
from {{ ref('encounters__stg_medical_claim') }} med
inner join {{ ref('encounters__stg_professional') }} prof on med.claim_line_id = prof.claim_line_id
inner join join_first_claim_dates dat on med.patient_id = dat.patient_id
and med.start_date between dat.encounter_start_date and dat.encounter_end_date