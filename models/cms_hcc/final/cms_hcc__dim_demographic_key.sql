{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with src as (

    select
          model_version
        , factor_type
        , enrollment_status
        , gender
        , age_group
        , medicaid_status
        , dual_status
        , orec
        , institutional_status
        , coefficient
    from {{ ref('cms_hcc__demographic_factors') }}
    where plan_segment is null

)

, base as (

    select
          model_version
        , factor_type
        , enrollment_status
        , gender
        , age_group
        , medicaid_status
        , dual_status
        , orec
        , institutional_status
        , coefficient
        , {{ cms_hcc_demographic_key(
              model_version,
              enrollment_status,
              gender,
              age_group,
              medicaid_status,
              dual_status,
              orec,
              institutional_status
          ) }} as demographic_key
    from src

)

, add_data_types as (

    select
          cast(demographic_key as {{ dbt.type_string() }}) as demographic_key
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(factor_type as {{ dbt.type_string() }}) as factor_type
        , cast(enrollment_status as {{ dbt.type_string() }}) as demographic_enrollment_status
        , cast(gender as {{ dbt.type_string() }}) as demographic_gender
        , cast(age_group as {{ dbt.type_string() }}) as demographic_age_group
        , cast(medicaid_status as {{ dbt.type_string() }}) as demographic_medicaid_status
        , cast(dual_status as {{ dbt.type_string() }}) as demographic_dual_status
        , cast(orec as {{ dbt.type_string() }}) as demographic_orec
        , cast(institutional_status as {{ dbt.type_string() }}) as demographic_institutional_status
        , round(cast(coefficient as {{ dbt.type_numeric() }}), 3) as coefficient
    from base

)

select
      demographic_key
    , model_version
    , factor_type
    , demographic_enrollment_status
    , demographic_gender
    , demographic_age_group
    , demographic_medicaid_status
    , demographic_dual_status
    , demographic_orec
    , demographic_institutional_status
    , coefficient
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
