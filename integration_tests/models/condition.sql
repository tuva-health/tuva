{{ config(
     enabled = the_tuva_project.tuva_boolean_var('clinical_enabled', false)
   )
}}

select
      source_condition_id
    , person_id
    , patient_id
    , encounter_id
    , recorded_date
    , onset_date
    , resolved_date
    , status
    , condition_type
    , code_system
    , source_code
    , source_description
    , condition_rank
    , present_on_admit_code
    , 'condition' as x_tuva_test_extension
    , 'condition' as ext_tuva_test_extension
    , ingest_datetime
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__condition') }}
