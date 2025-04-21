{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      person_id as patient_internal_Id
    , first_name as name_first
    , last_name as name_last
    , sex as gender
    , race
    , birth_date
from {{ ref('fhir_preprocessing__stg_core__patient') }}
