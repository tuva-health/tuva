{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}
with unioned as (

    {{ dbt_utils.union_relations(

        relations=[
            ref('fhir_preprocessing__int_medical_claim_eob'),
            ref('fhir_preprocessing__int_pharmacy_claim_eob')
        ]

    ) }}

)

select
      cast(patient_internal_id as {{ dbt.type_string() }} ) as patient_internal_id
    , cast(resource_internal_id as {{ dbt.type_string() }} ) as resource_internal_id
    , cast(unique_claim_id as {{ dbt.type_string() }} ) as unique_claim_id
    , cast(eob_type_code as {{ dbt.type_string() }} ) as eob_type_code
    , cast(eob_subtype_code as {{ dbt.type_string() }} ) as eob_subtype_code
    , cast(eob_billable_period_start as date) as eob_billable_period_start
    , cast(eob_billable_period_end as date) as eob_billable_period_end
    , cast(eob_created as date) as eob_created
    , cast(organization_name as {{ dbt.type_string() }} ) as organization_name
    , cast(practitioner_internal_id as {{ dbt.type_string() }} ) as practitioner_internal_id
    , cast(practitioner_name_text as {{ dbt.type_string() }} ) as practitioner_name_text
    , cast(coverage_internal_id as {{ dbt.type_string() }} ) as coverage_internal_id
    , cast(eob_diagnosis_list as {{ dbt.type_string() }} ) as eob_diagnosis_list
    , cast(eob_procedure_list as {{ dbt.type_string() }} ) as eob_procedure_list
    , cast(eob_supporting_info_list as {{ dbt.type_string() }} ) as eob_supporting_info_list
    , cast(eob_item_list as {{ dbt.type_string() }} ) as eob_item_list
    , cast(eob_total_list as {{ dbt.type_string() }} ) as eob_total_list
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    , cast('{{ the_tuva_project.get_tuva_package_version() }}' as {{ dbt.type_string() }} ) as tuva_package_version
from unioned
