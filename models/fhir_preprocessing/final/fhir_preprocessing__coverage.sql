{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

select
      coverage.patient_internal_id
    , coverage.resource_internal_id
    , coverage.organization_name
    , coverage.coverage_plan
    , coverage.coverage_period_start
    , coverage.coverage_period_end
    , coverage.coverage_relationship
    , coverage.coverage_status
    , coverage.coverage_subscriber_id
    , coverage.data_source
    , coverage_type.coverage_type_list
from {{ ref('fhir_preprocessing__int_coverage') }} as coverage
    left outer join {{ ref('fhir_preprocessing__int_coverage_type') }} as coverage_type
        on coverage.patient_internal_id = coverage_type.patient_internal_id
        and coverage.resource_internal_id = coverage_type.resource_internal_id
