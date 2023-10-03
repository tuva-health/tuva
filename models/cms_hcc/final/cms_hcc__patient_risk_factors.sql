{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}
with demographic_factors as (

    select
          patient_id
        /* concatenate demographic risk factors */
        , gender
            || ', '
            || age_group
            || ' Years'
            || ', '
            || enrollment_status
            || ' Enrollee'
            || ', '
            || case
                when medicaid_status = 'Yes' then 'Medicaid'
                else 'Non-Medicaid'
                end
            || ', '
            || dual_status
            || ' Dual'
            || ', '
            || orec
            || ', '
            || case
                when institutional_status = 'Yes' then 'Institutional'
                else 'Non-Institutional'
                end
          as description
        , coefficient
        , factor_type
        , model_version
        , payment_year
    from {{ ref('cms_hcc__int_demographic_factors') }}

)

, demographic_defaults as (

    select
          patient_id
        , enrollment_status_default
        , medicaid_dual_status_default
        , orec_default
        , institutional_status_default
    from {{ ref('cms_hcc__int_demographic_factors') }}

)

, disease_factors as (

    select
          patient_id
        , hcc_description || ' (HCC ' || hcc_code || ')' as description
        , coefficient
        , factor_type
        , model_version
        , payment_year
    from {{ ref('cms_hcc__int_disease_factors') }}

)

, enrollment_interactions as (

    select
          patient_id
        , description
        , coefficient
        , factor_type
        , model_version
        , payment_year
    from {{ ref('cms_hcc__int_enrollment_interaction_factors') }}

)

, disabled_interactions as (

    select
          patient_id
        , description
        , coefficient
        , factor_type
        , model_version
        , payment_year
    from {{ ref('cms_hcc__int_disabled_interaction_factors') }}

)

, disease_interactions as (

    select
          patient_id
        , description
        , coefficient
        , factor_type
        , model_version
        , payment_year
    from {{ ref('cms_hcc__int_disease_interaction_factors') }}

)

, hcc_counts as (

    select
          patient_id
        , description
        , coefficient
        , factor_type
        , model_version
        , payment_year
    from {{ ref('cms_hcc__int_hcc_count_factors') }}

)

, unioned as (

    select * from demographic_factors
    union all
    select * from disease_factors
    union all
    select * from enrollment_interactions
    union all
    select * from disabled_interactions
    union all
    select * from disease_interactions
    union all
    select * from hcc_counts

)

, add_defaults as (

    select
          unioned.patient_id
        , demographic_defaults.enrollment_status_default
        , demographic_defaults.medicaid_dual_status_default
        , demographic_defaults.orec_default
        , demographic_defaults.institutional_status_default
        , unioned.description as risk_factor_description
        , unioned.coefficient
        , unioned.factor_type
        , unioned.model_version
        , unioned.payment_year
    from unioned
         left join demographic_defaults
         on unioned.patient_id = demographic_defaults.patient_id

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(enrollment_status_default as boolean) as enrollment_status_default
        , cast(medicaid_dual_status_default as boolean) as medicaid_dual_status_default
        , cast(orec_default as boolean) as orec_default
        , cast(institutional_status_default as boolean) as institutional_status_default
        , cast(factor_type as {{ dbt.type_string() }}) as factor_type
        , cast(risk_factor_description as {{ dbt.type_string() }}) as risk_factor_description
        , round(cast(coefficient as {{ dbt.type_numeric() }}),3) as coefficient
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
    from add_defaults

)

select
      patient_id
    , enrollment_status_default
    , medicaid_dual_status_default
    , orec_default
    , institutional_status_default
    , factor_type
    , risk_factor_description
    , coefficient
    , model_version
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types