{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      procedure_id
    , person_id
    , claim_id
    , encounter_id
    , normalized_code_type
    , normalized_code
    , normalized_description
    , source_code_type
    , source_code
    , source_description
    , procedure_date
    , practitioner_id
    , data_source
from {{ ref('core__procedure') }}
