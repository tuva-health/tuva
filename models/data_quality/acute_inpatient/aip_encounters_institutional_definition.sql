{{ config(
    enabled = var('claims_enabled', False)
) }}

with single_claim_encounters as (

    select
          patient_id
        , encounter_id
        , merge_start_date as encounter_start_date
        , merge_end_date as encounter_end_date
        , ms_drg_code
        , apr_drg_code
        , diagnosis_code_1
        , admit_type_code
        , admit_source_code
        , discharge_disposition_code
        , facility_npi
        , rendering_npi
        , paid_amount
        , dq_problem
        , usable_diagnosis_code_1
        , usable_admit_type_code
        , usable_admit_source_code
        , usable_discharge_disposition_code
        , usable_facility_npi
        , usable_rendering_npi
        , 1 as single_claim_encounter
        , 0 as multi_claim_encounter
    from {{ ref('aip_single_claim_encounters') }}
    where usable_for_aip_encounter = 1

)

, multi_claim_encounters as (

    select
          patient_id
        , encounter_id
        , encounter_start_date
        , encounter_end_date
        , ms_drg_code
        , apr_drg_code
        , diagnosis_code_1
        , admit_type_code
        , admit_source_code
        , discharge_disposition_code
        , facility_npi
        , rendering_npi
        , paid_amount
        , dq_problem
        , usable_diagnosis_code_1
        , usable_admit_type_code
        , usable_admit_source_code
        , usable_discharge_disposition_code
        , usable_facility_npi
        , usable_rendering_npi
        , 0 as single_claim_encounter
        , 1 as multi_claim_encounter
    from {{ ref('aip_multiple_claim_encounter_fields') }}

)

select
      patient_id
    , encounter_id
    , encounter_start_date
    , encounter_end_date
    , ms_drg_code
    , apr_drg_code
    , diagnosis_code_1
    , admit_type_code
    , admit_source_code
    , discharge_disposition_code
    , facility_npi
    , rendering_npi
    , paid_amount
    , dq_problem
    , usable_diagnosis_code_1
    , usable_admit_type_code
    , usable_admit_source_code
    , usable_discharge_disposition_code
    , usable_facility_npi
    , usable_rendering_npi
    , single_claim_encounter
    , multi_claim_encounter
from single_claim_encounters

union all

select
      patient_id
    , encounter_id
    , encounter_start_date
    , encounter_end_date
    , ms_drg_code
    , apr_drg_code
    , diagnosis_code_1
    , admit_type_code
    , admit_source_code
    , discharge_disposition_code
    , facility_npi
    , rendering_npi
    , paid_amount
    , dq_problem
    , usable_diagnosis_code_1
    , usable_admit_type_code
    , usable_admit_source_code
    , usable_discharge_disposition_code
    , usable_facility_npi
    , usable_rendering_npi
    , single_claim_encounter
    , multi_claim_encounter
from multi_claim_encounters
