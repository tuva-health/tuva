{{ config(
     enabled = var('medical_records_enabled',var('tuva_marts_enabled',False))
   )
}}

select 
    encounter_id
    , patient_id
    , encounter_type
    , encounter_start_date
    , encounter_end_date
    , length_of_stay
    , admit_source_code
    , admit_source_description
    , admit_type_code
    , admit_type_description
    , discharge_disposition_code
    , discharge_disposition_description
    , attending_provider_id
    , facility_npi
    , primary_diagnosis_code
    , primary_diagnosis_description
    , ms_drg_code
    , ms_drg_description
    , apr_drg_code
    , apr_drg_description
    , paid_amount
    , allowed_amount
    , charge_amount
    , data_source
    , tuva_last_run
from {{ ref('core_stage_clinical__encounter') }}