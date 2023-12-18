{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with first_claim_values as(
    select distinct
        e.encounter_id
        , coalesce(claim_start_date, admission_date) as claim_start
        , diagnosis_code_1
        , diagnosis_code_type
        , admit_source_code
        , admit_type_code
        , facility_npi
        , ms_drg_code
        , apr_drg_code
    from {{ ref('emergency_department__int_encounter_id')}} e
    inner join {{ ref('emergency_department__stg_medical_claim') }} m
        on e.claim_id = m.claim_id
    where claim_type = 'institutional'
)

select
    encounter_id
    , claim_start
    , diagnosis_code_1
    , diagnosis_code_type
    , admit_source_code
    , admit_type_code
    , facility_npi
    , ms_drg_code
    , apr_drg_code
    , row_number() over (partition by encounter_id order by claim_start) as claim_row
from first_claim_values
