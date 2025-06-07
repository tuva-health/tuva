{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}
with unioned as (

    {{ dbt_utils.union_relations(

        relations=[
            ref('fhir_preprocessing__int_lab_result'),
            ref('fhir_preprocessing__int_observation')
        ]

    ) }}

)

select
      cast(patient_internal_id as {{ dbt.type_string() }} ) as patient_internal_id
    , cast(resource_internal_id as {{ dbt.type_string() }} ) as resource_internal_id
    , cast(encounter_internal_id as {{ dbt.type_string() }} ) as encounter_internal_id
    , cast(observation_status as {{ dbt.type_string() }} ) as observation_status
    , cast(observation_category as {{ dbt.type_string() }} ) as observation_category
    , cast(observation_code_system as {{ dbt.type_string() }} ) as observation_code_system
    , cast(observation_code as {{ dbt.type_string() }} ) as observation_code
    , cast(observation_code_text as {{ dbt.type_string() }} ) as observation_code_text
    , cast(observation_datetime as {{ dbt.type_timestamp() }} ) as observation_datetime
    , cast(observation_value as {{ dbt.type_string() }} ) as observation_value
    , cast(observation_value_units as {{ dbt.type_string() }} ) as observation_value_units
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    , cast('{{ the_tuva_project.get_tuva_package_version() }}' as {{ dbt.type_string() }} ) as tuva_package_version
from unioned
