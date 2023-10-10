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
    from {{ ref('acute_inpatient__encounter_id')}} e
    inner join medicare_lds_five_percent._tuva_claims.medical_claim m
        on e.claim_id = m.claim_id
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
