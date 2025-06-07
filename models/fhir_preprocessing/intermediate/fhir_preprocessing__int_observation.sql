{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}
select
      cast(person_id as {{ dbt.type_string() }} ) as patient_internal_id
    , cast({{ dbt_utils.generate_surrogate_key(['observation_id']) }} as {{ dbt.type_string() }} ) as resource_internal_id
    , cast(encounter_id as {{ dbt.type_string() }} ) as encounter_internal_id
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
    , upper(
        coalesce(
          cast(normalized_code_type as {{ dbt.type_string() }} )
        , cast(source_code_type as {{ dbt.type_string() }} )
        )
      ) as observation_code_system
    , coalesce(
          cast(normalized_code as {{ dbt.type_string() }} )
        , cast(source_code as {{ dbt.type_string() }} )
      ) as observation_code
    , coalesce(
          cast(normalized_description as {{ dbt.type_string() }} )
        , cast(source_description as {{ dbt.type_string() }} )
      ) as observation_code_text
    , cast(observation_date as {{ dbt.type_timestamp() }} ) as observation_datetime
    , cast(result as {{ dbt.type_string() }} ) as observation_value
    , coalesce(
          cast(normalized_units as {{ dbt.type_string() }} )
        , cast(source_units as {{ dbt.type_string() }} )
      ) as observation_value_units
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
from {{ ref('fhir_preprocessing__stg_core__observation') }}
