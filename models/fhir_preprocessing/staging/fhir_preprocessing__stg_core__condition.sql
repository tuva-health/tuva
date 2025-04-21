{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
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
from {{ ref('core__condition') }}
