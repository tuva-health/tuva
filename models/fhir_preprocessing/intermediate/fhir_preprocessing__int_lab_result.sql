{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      person_id as patient_internal_id
    , lab_result_id as resource_internal_id
    , encounter_id as encounter_internal_id
    , case
        when lower(status) in ('final', 'f') then 'final'
        when lower(status) in ('preliminary', 'p') then 'preliminary'
        when lower(status) in ('corrected', 'c') then 'corrected'
        when lower(status) in ('cancelled', 'd') then 'cancelled'
        else lower(status)
      end as observation_status
    , 'laboratory' as observation_category
    , upper(coalesce(normalized_code_type,source_code_type)) as observation_code_system
    , coalesce(normalized_code,source_code) as observation_code
    , coalesce(normalized_description,source_description) as observation_code_text
    , result_date as observation_datetime
    , result as observation_value
    , coalesce(normalized_units,source_units) as observation_value_units
from {{ ref('fhir_preprocessing__stg_core__lab_result') }}
