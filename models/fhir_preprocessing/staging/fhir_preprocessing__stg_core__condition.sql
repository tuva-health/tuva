{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}
select
      condition_id
    , person_id
    , claim_id
    , encounter_id
    , recorded_date
    , onset_date
    , resolved_date
    , status
    , normalized_code_type
    , normalized_code
    , normalized_description
    , condition_rank
    , data_source
from {{ ref('core__condition') }}
