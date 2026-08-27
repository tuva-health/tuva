{{ config(
     enabled = the_tuva_project.tuva_boolean_var('clinical_enabled', false)
   )
}}

select
      encounter_id
    , person_id
    , patient_id
    , encounter_type
    , encounter_start_date
    , encounter_end_date
    , admit_source_code
    , admit_type_code
    , discharge_disposition_code
    , attending_provider_id
    , attending_provider_name
    , facility_npi
    , facility_name
    , primary_diagnosis_code_type
    , primary_diagnosis_code
    , drg_code_type
    , drg_code
    , paid_amount
    , allowed_amount
    , charge_amount
    , 'encounter' as x_tuva_test_extension
    , 'encounter' as ext_tuva_test_extension
    , ingest_datetime
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__encounter') }}
