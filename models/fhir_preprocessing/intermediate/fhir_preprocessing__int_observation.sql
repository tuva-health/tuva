{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      person_id as patient_internal_id
    , {{ dbt_utils.generate_surrogate_key(['observation_id']) }} as resource_internal_id
    , encounter_id as encounter_internal_id
    , 'final' as observation_status
    , case
        when lower(observation_type) like '%social%' then 'social-history'
        when lower(observation_type) like '%vital%' then 'vital-signs'
        when lower(observation_type) like '%imaging%' then 'imaging'
        when lower(observation_type) like '%laboratory%' then 'laboratory'
        when lower(observation_type) like '%procedure%' then 'procedure'
        when lower(observation_type) like '%survey%' then 'survey'
        when lower(observation_type) like '%exam%' then 'exam'
        when lower(observation_type) like '%therapy%' then 'therapy'
        when lower(observation_type) like '%activity%' then 'activity'
        else 'other'
      end as observation_category
    , upper(coalesce(normalized_code_type,source_code_type)) as observation_code_system
    , coalesce(normalized_code,source_code) as observation_code
    , coalesce(normalized_description,source_description) as observation_code_text
    , observation_date as observation_datetime
    , result as observation_value
    , coalesce(normalized_units,source_units) as observation_value_units
    , data_source
from {{ ref('fhir_preprocessing__stg_core__observation') }}
