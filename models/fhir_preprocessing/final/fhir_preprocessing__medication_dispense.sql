{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}
select
      cast(person_id as {{ dbt.type_string() }} ) as patient_internal_id
    , cast(medication_id as {{ dbt.type_string() }} ) as resource_internal_id
    , 'completed' as medication_dispense_status
    , case
        when ndc_code is not null then 'NDC'
        when rxnorm_code is not null then 'RXNORM'
        else cast(source_code_type as {{ dbt.type_string() }} )
      end as medication_code_system
    , coalesce(
          cast(ndc_code as {{ dbt.type_string() }} )
        , cast(rxnorm_code as {{ dbt.type_string() }} )
      ) as medication_code
    , coalesce(
          cast(ndc_description as {{ dbt.type_string() }} )
        , cast(rxnorm_description as {{ dbt.type_string() }} )
      ) as medication_code_display
    , cast(days_supply as {{ dbt.type_numeric() }} ) as medication_dispense_days_supply_value
    , cast(dispensing_date as date) as medication_dispense_when_handed_over
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    , cast('{{ the_tuva_project.get_tuva_package_version() }}' as {{ dbt.type_string() }} ) as tuva_package_version
from {{ ref('fhir_preprocessing__stg_core__medication') }}
