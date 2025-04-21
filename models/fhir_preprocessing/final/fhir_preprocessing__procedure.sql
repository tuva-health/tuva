{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      person_id as patient_internal_id
    , procedure_id as resource_internal_id
    , 'completed' as procedureStatus
    , case
        when lower(coalesce(normalized_code_type, source_code_type)) = 'icd-10-pcs' then 'ICD10'
        when lower(coalesce(normalized_code_type, source_code_type)) = 'icd-9-pcs' then 'ICD9'
        else coalesce(normalized_code_type, source_code_type)
      end as procedure_code_system
    , coalesce(normalized_code, source_code) as procedure_code
    , coalesce(normalized_description, source_description) as procedure_display
    , procedure_date as procedure_performed_datetime
    , practitioner_id as practitionerNPI
from {{ ref('fhir_preprocessing__stg_core__procedure') }}
where procedure_id is not null
