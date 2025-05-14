{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

select
      cast(coverage.patient_internal_id as {{ dbt.type_string() }} ) as patient_internal_id
    , cast(coverage.resource_internal_id as {{ dbt.type_string() }} ) as resource_internal_id
    , cast(coverage.organization_name as {{ dbt.type_string() }} ) as organization_name
    , cast(coverage.coverage_plan as {{ dbt.type_string() }} ) as coverage_plan
    , cast(coverage.coverage_period_start as date) as coverage_period_start
    , cast(coverage.coverage_period_end as date) as coverage_period_end
    , cast(coverage.coverage_relationship as {{ dbt.type_string() }} ) as coverage_relationship
    , cast(coverage.coverage_status as {{ dbt.type_string() }} ) as coverage_status
    , cast(coverage.coverage_subscriber_id as {{ dbt.type_string() }} ) as coverage_subscriber_id
    , cast(coverage.data_source as {{ dbt.type_string() }} ) as data_source
    , cast(coverage_type.coverage_type_list as {{ dbt.type_string() }} ) as coverage_type_list
from {{ ref('fhir_preprocessing__int_coverage') }} as coverage
    left outer join {{ ref('fhir_preprocessing__int_coverage_type') }} as coverage_type
        on coverage.patient_internal_id = coverage_type.patient_internal_id
        and coverage.resource_internal_id = coverage_type.resource_internal_id
