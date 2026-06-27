{{ config(
     enabled = var('clinical_enabled', False) | as_bool
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
    , ingest_datetime
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__condition') }}
