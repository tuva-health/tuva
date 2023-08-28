{{ config(
     enabled = var('medical_records_enabled',var('tuva_marts_enabled',False))
   )
}}

select
    procedure_id
    , patient_id
    , encounter_id
    , claim_id
    , procedure_date
    , source_code_type
    , source_code
    , source_description
    , normalized_code_type
    , normalized_code
    , normalized_description
    , modifier_1
    , modifier_2
    , modifier_3
    , modifier_4
    , modifier_5
    , practitioner_id
    , data_source
    , tuva_last_run
from {{ ref('procedure') }}