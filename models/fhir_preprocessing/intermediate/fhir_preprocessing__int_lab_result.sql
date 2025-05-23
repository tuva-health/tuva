{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}
select
      cast(person_id as {{ dbt.type_string() }} ) as patient_internal_id
    , cast({{ dbt_utils.generate_surrogate_key(['lab_result_id']) }} as {{ dbt.type_string() }} ) as resource_internal_id
    , cast(encounter_id as {{ dbt.type_string() }} ) as encounter_internal_id
    , case
        when lower(status) in ('final', 'f') then 'final'
        when lower(status) in ('preliminary', 'p') then 'preliminary'
        when lower(status) in ('corrected', 'c') then 'corrected'
        when lower(status) in ('cancelled', 'd') then 'cancelled'
        else lower(status)
      end as observation_status
    , 'laboratory' as observation_category
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
    , cast(result_date as {{ dbt.type_timestamp() }} ) as observation_datetime
    , cast(result as {{ dbt.type_string() }} ) as observation_value
    , coalesce(
          cast(normalized_units as {{ dbt.type_string() }} )
        , cast(source_units as {{ dbt.type_string() }} )
      ) as observation_value_units
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
from {{ ref('fhir_preprocessing__stg_core__lab_result') }}
