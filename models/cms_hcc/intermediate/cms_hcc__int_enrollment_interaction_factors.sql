{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
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
        , model_version
        , payment_year
    from {{ ref('cms_hcc__int_demographic_factors') }}

)

, seed_interaction_factors as (

    select
          model_version
        , factor_type
        , gender
        , enrollment_status
        , medicaid_status
        , dual_status
        , institutional_status
        , description
        , coefficient
    from {{ ref('cms_hcc__enrollment_interaction_factors') }}
    where model_version = '{{ model_version_compiled }}'

)

/*
    Originally disabled interactions for non-institutional members >= 65
*/
, non_institutional_interactions as (

    select
          demographics.patient_id
        , demographics.model_version
        , demographics.payment_year
        , seed_interaction_factors.factor_type
        , seed_interaction_factors.description
        , seed_interaction_factors.coefficient
    from demographics
         inner join seed_interaction_factors
         on demographics.gender = seed_interaction_factors.gender
         and demographics.enrollment_status = seed_interaction_factors.enrollment_status
         and demographics.medicaid_status = seed_interaction_factors.medicaid_status
         and demographics.dual_status = seed_interaction_factors.dual_status
         and demographics.institutional_status = seed_interaction_factors.institutional_status
    where demographics.institutional_status = 'No'
    and demographics.orec = 'Disabled'
    and demographics.age_group in (
          '65-69'
        , '70-74'
        , '75-79'
        , '80-84'
        , '85-89'
        , '90-94'
        , '>=95'
    )

)

/*
    Medicaid interactions for institutional members
*/
, institutional_interactions as (

    select
          demographics.patient_id
        , demographics.model_version
        , demographics.payment_year
        , seed_interaction_factors.factor_type
        , seed_interaction_factors.description
        , seed_interaction_factors.coefficient
    from demographics
         inner join seed_interaction_factors
         on demographics.enrollment_status = seed_interaction_factors.enrollment_status
         and demographics.institutional_status = seed_interaction_factors.institutional_status
    where demographics.institutional_status = 'Yes'
    and demographics.medicaid_status = 'Yes'

)

, unioned as (

    select * from non_institutional_interactions
    union all
    select * from institutional_interactions

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(description as {{ dbt.type_string() }}) as description
        , round(cast(coefficient as {{ dbt.type_numeric() }}),3) as coefficient
        , cast(factor_type as {{ dbt.type_string() }}) as factor_type
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
    from unioned

)

select
      patient_id
    , description
    , coefficient
    , factor_type
    , model_version
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types