with first_claim_dates as (
    select
        enc.patient_data_source_id
        , enc.claim_id
        , enc.encounter_id
        , dat.encounter_start_date
        , dat.encounter_end_date
    from {{ ref('encounters__int_emergency_department__generate_encounter_id') }} as enc
        inner join {{ ref('encounters__int_emergency_department__start_end_dates') }} as dat
        on enc.encounter_id = dat.encounter_id
    where enc.encounter_claim_number = 1
)

-- ensuring each claim is only attributed to one encounter with claim_attribution_number
, inst_and_prof as (
    select
        dat.encounter_id
        , dat.encounter_start_date
        , dat.encounter_end_date
        , med.medical_claim_sk
        , med.claim_id
        , med.claim_line_number
    from {{ ref('encounters__stg_medical_claim') }} as med
        inner join {{ ref('encounters__stg_professional') }} as prof
        on med.medical_claim_sk = prof.medical_claim_sk
        inner join first_claim_dates as dat
        on med.patient_data_source_id = dat.patient_data_source_id
        and med.start_date between dat.encounter_start_date and dat.encounter_end_date

    union all

    select dat.encounter_id
        , dat.encounter_start_date
        , dat.encounter_end_date
        , med.medical_claim_sk
        , med.claim_id
        , med.claim_line_number
    from {{ ref('encounters__stg_medical_claim') }} as med
        inner join {{ ref('encounters__stg_outpatient_institutional') }} as inst
        on med.medical_claim_sk = inst.medical_claim_sk
        inner join first_claim_dates as dat
        on med.patient_data_source_id = dat.patient_data_source_id
        and med.start_date between dat.encounter_start_date and dat.encounter_end_date
    where dat.claim_id <> med.claim_id
)

select
    medical_claim_sk
    , encounter_id
    , encounter_start_date
    , encounter_end_date
    , claim_id
    , claim_line_number
    , row_number() over (partition by medical_claim_sk order by encounter_id) as claim_attribution_number
from inst_and_prof
