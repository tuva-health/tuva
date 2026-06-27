{{ config(
     enabled = var('clinical_enabled', False) | as_bool
   )
}}

select
      source_procedure_id
    , person_id
    , patient_id
    , encounter_id
    , procedure_date
    , code_system
    , source_code
    , source_description
    , modifier_1
    , modifier_2
    , modifier_3
    , modifier_4
    , modifier_5
    , practitioner_id
    , ingest_datetime
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__procedure') }}
