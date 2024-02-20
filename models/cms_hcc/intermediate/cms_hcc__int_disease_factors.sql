{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

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
        , model_version
        , payment_year
    from {{ ref('cms_hcc__int_demographic_factors') }}

)

, hcc_hierarchy as (

    select
          patient_id
        , hcc_code
        , model_version
    from {{ ref('cms_hcc__int_hcc_hierarchy') }}

)

, seed_disease_factors as (

    select
          model_version
        , factor_type
        , enrollment_status
        , medicaid_status
        , dual_status
        , orec
        , institutional_status
        , hcc_code
        , description
        , coefficient
    from {{ ref('cms_hcc__disease_factors') }}

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
        , demographics.model_version
        , demographics.payment_year
        , hcc_hierarchy.hcc_code
    from demographics
        inner join hcc_hierarchy
            on demographics.patient_id = hcc_hierarchy.patient_id
            and demographics.model_version = hcc_hierarchy.model_version

)

, disease_factors as (

    select
          demographics_with_hccs.patient_id
        , demographics_with_hccs.hcc_code
        , demographics_with_hccs.model_version
        , demographics_with_hccs.payment_year
        , seed_disease_factors.factor_type
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
            and demographics_with_hccs.model_version = seed_disease_factors.model_version

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(description as {{ dbt.type_string() }}) as hcc_description
        , round(cast(coefficient as {{ dbt.type_numeric() }}),3) as coefficient
        , cast(factor_type as {{ dbt.type_string() }}) as factor_type
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
    from disease_factors

)

select
      patient_id
    , hcc_code
    , hcc_description
    , coefficient
    , factor_type
    , model_version
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types