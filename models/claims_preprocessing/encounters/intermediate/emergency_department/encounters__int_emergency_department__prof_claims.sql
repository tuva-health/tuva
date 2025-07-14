-- Step 1: Get encounter date ranges with patient information
-- We only need one row per encounter, so get encounter grain
with encounters__prof_and_lower_priority as (
    select *
    from {{ ref('encounters__stg_prof_and_lower_priority') }}
),
encounters__stg_outpatient_institutional as (
    select *
    from {{ ref('encounters__stg_outpatient_institutional') }}
),
encounters as (
    select distinct
        gei.encounter_id
        , gei.patient_sk
        , gei.data_source
        , gei.encounter_start_date
        , gei.encounter_end_date
    from {{ ref('encounters__int_emergency_department__generate_encounter_id') }} gei
)

-- ensuring each claim is only attributed to one encounter with claim_attribution_number
, inst_and_prof as (
    select med.medical_claim_sk
        , med.data_source
        , med.claim_id
        , med.claim_line_number
        , med.patient_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
    from encounters__prof_and_lower_priority as med
        inner join encounters as enc
        on med.patient_sk = enc.patient_sk
        and med.start_date between enc.encounter_start_date and enc.encounter_end_date

    union all

    select med.medical_claim_sk
        , med.data_source
        , med.claim_id
        , med.claim_line_number
        , med.patient_sk
        , enc.encounter_id
        , enc.encounter_start_date
        , enc.encounter_end_date
    from encounters__stg_outpatient_institutional as med
        inner join encounters as enc
        on med.patient_sk = enc.patient_sk
        and med.start_date between enc.encounter_start_date and enc.encounter_end_date
--        and med.claim_id <> enc.claim_id
)

select medical_claim_sk
    , data_source
    , claim_id
--    , claim_line_number
    , patient_sk
    , encounter_id
    , encounter_start_date
    , encounter_end_date
    , row_number() over (partition by medical_claim_sk order by encounter_id) as claim_attribution_number
from inst_and_prof
