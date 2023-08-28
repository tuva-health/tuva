{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
   )
}}

select
    condition_id
    , patient_id
    , encounter_id
    , claim_id
    , recorded_date
    , onset_date
    , resolved_date
    , status
    , condition_type
    , source_code_type
    , source_code
    , source_description
    , normalized_code_type
    , normalized_code
    , normalized_description
    , condition_rank
    , present_on_admit_code
    , present_on_admit_description
    , data_source
    , tuva_last_run
from {{ ref('condition') }}