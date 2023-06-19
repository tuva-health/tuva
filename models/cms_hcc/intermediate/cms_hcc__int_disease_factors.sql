{{ config(
     enabled = var('cms_hcc_enabled',var('tuva_marts_enabled',True))
   )
}}
/*
The hcc_model_version var has been set here so it gets compiled.
*/

{% set model_version_compiled = var('cms_hcc_model_version') -%}

with demographics as (

    select
          patient_id
        , enrollment_status
        , gender
        , age_group
        , medicaid_status
        , dual_status
        , orec
        , institutional_status
        , enrollment_status_default
        , medicaid_dual_status_default
        , institutional_status_default
        , model_version
        , payment_year
    from {{ ref('cms_hcc__int_demographic_factors') }}

)

, hcc_hierarchy as (

    select
          patient_id
        , hcc_code
    from {{ ref('cms_hcc__int_hcc_hierarchy') }}

)

, seed_disease_factors as (

    select
          model_version
        , enrollment_status
        , medicaid_status
        , dual_status
        , orec
        , institutional_status
        , hcc_code
        , description
        , coefficient
    from {{ ref('cms_hcc__disease_factors') }}
    where model_version = '{{ model_version_compiled }}'

)

, demographics_with_hccs as (

    select
          demographics.patient_id
        , demographics.enrollment_status
        , demographics.gender
        , demographics.age_group
        , demographics.medicaid_status
        , demographics.dual_status
        , demographics.orec
        , demographics.institutional_status
        , demographics.enrollment_status_default
        , demographics.medicaid_dual_status_default
        , demographics.institutional_status_default
        , demographics.model_version
        , demographics.payment_year
        , hcc_hierarchy.hcc_code
    from demographics
         inner join hcc_hierarchy
         on demographics.patient_id = hcc_hierarchy.patient_id

)

, disease_factors as (

    select
          demographics_with_hccs.patient_id
        , demographics_with_hccs.hcc_code
        , demographics_with_hccs.model_version
        , demographics_with_hccs.payment_year
        , seed_disease_factors.description
        , seed_disease_factors.coefficient
    from demographics_with_hccs
         inner join seed_disease_factors
         on demographics_with_hccs.enrollment_status = seed_disease_factors.enrollment_status
         and demographics_with_hccs.medicaid_status = seed_disease_factors.medicaid_status
         and demographics_with_hccs.dual_status = seed_disease_factors.dual_status
         and demographics_with_hccs.orec = seed_disease_factors.orec
         and demographics_with_hccs.institutional_status = seed_disease_factors.institutional_status
         and demographics_with_hccs.hcc_code = seed_disease_factors.hcc_code

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(description as {{ dbt.type_string() }}) as hcc_description
        , round(cast(coefficient as {{ dbt.type_numeric() }}),3) as coefficient
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
        , cast('{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as {{ dbt.type_timestamp() }}) as date_calculated
    from disease_factors

)

select
      patient_id
    , hcc_code
    , hcc_description
    , coefficient
    , model_version
    , payment_year
    , '{{ var('last_update')}}' as last_update
from add_data_types