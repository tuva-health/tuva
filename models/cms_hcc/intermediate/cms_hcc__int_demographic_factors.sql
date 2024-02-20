{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with members as (

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
        , orec_default
        , institutional_status_default
        , payment_year
    from {{ ref('cms_hcc__int_members') }}

)

, seed_demographic_factors as (

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
    where plan_segment is null /* data not available */

)

, v24_new_enrollees as (

    select
          members.patient_id
        , members.enrollment_status
        , members.gender
        , members.age_group
        , members.medicaid_status
        , members.dual_status
        , members.orec
        , members.institutional_status
        , members.enrollment_status_default
        , members.medicaid_dual_status_default
        , members.orec_default
        , members.institutional_status_default
        , members.payment_year
        , seed_demographic_factors.model_version
        , seed_demographic_factors.factor_type
        , seed_demographic_factors.coefficient
    from members
        inner join seed_demographic_factors
            on members.enrollment_status = seed_demographic_factors.enrollment_status
            and members.gender = seed_demographic_factors.gender
            and members.age_group = seed_demographic_factors.age_group
            and members.medicaid_status = seed_demographic_factors.medicaid_status
            and members.orec = seed_demographic_factors.orec
    where members.enrollment_status = 'New'
        and seed_demographic_factors.model_version = 'CMS-HCC-V24'

)

, v24_continuining_enrollees as (

    select
          members.patient_id
        , members.enrollment_status
        , members.gender
        , members.age_group
        , members.medicaid_status
        , members.dual_status
        , members.orec
        , members.institutional_status
        , members.enrollment_status_default
        , members.medicaid_dual_status_default
        , members.orec_default
        , members.institutional_status_default
        , members.payment_year
        , seed_demographic_factors.model_version
        , seed_demographic_factors.factor_type
        , seed_demographic_factors.coefficient
    from members
        inner join seed_demographic_factors
            on members.enrollment_status = seed_demographic_factors.enrollment_status
            and members.gender = seed_demographic_factors.gender
            and members.age_group = seed_demographic_factors.age_group
            and members.medicaid_status = seed_demographic_factors.medicaid_status
            and members.dual_status = seed_demographic_factors.dual_status
            and members.orec = seed_demographic_factors.orec
            and members.institutional_status = seed_demographic_factors.institutional_status
    where members.enrollment_status = 'Continuing'
        and seed_demographic_factors.model_version = 'CMS-HCC-V24'

)

, v28_new_enrollees as (

    select
          members.patient_id
        , members.enrollment_status
        , members.gender
        , members.age_group
        , members.medicaid_status
        , members.dual_status
        , members.orec
        , members.institutional_status
        , members.enrollment_status_default
        , members.medicaid_dual_status_default
        , members.orec_default
        , members.institutional_status_default
        , members.payment_year
        , seed_demographic_factors.model_version
        , seed_demographic_factors.factor_type
        , seed_demographic_factors.coefficient
    from members
        inner join seed_demographic_factors
            on members.enrollment_status = seed_demographic_factors.enrollment_status
            and members.gender = seed_demographic_factors.gender
            and members.age_group = seed_demographic_factors.age_group
            and members.medicaid_status = seed_demographic_factors.medicaid_status
            and members.orec = seed_demographic_factors.orec
    where members.enrollment_status = 'New'
        and seed_demographic_factors.model_version = 'CMS-HCC-V28'

)

, v28_continuining_enrollees as (

    select
          members.patient_id
        , members.enrollment_status
        , members.gender
        , members.age_group
        , members.medicaid_status
        , members.dual_status
        , members.orec
        , members.institutional_status
        , members.enrollment_status_default
        , members.medicaid_dual_status_default
        , members.orec_default
        , members.institutional_status_default
        , members.payment_year
        , seed_demographic_factors.model_version
        , seed_demographic_factors.factor_type
        , seed_demographic_factors.coefficient
    from members
        inner join seed_demographic_factors
            on members.enrollment_status = seed_demographic_factors.enrollment_status
            and members.gender = seed_demographic_factors.gender
            and members.age_group = seed_demographic_factors.age_group
            and members.medicaid_status = seed_demographic_factors.medicaid_status
            and members.dual_status = seed_demographic_factors.dual_status
            and members.orec = seed_demographic_factors.orec
            and members.institutional_status = seed_demographic_factors.institutional_status
    where members.enrollment_status = 'Continuing'
        and seed_demographic_factors.model_version = 'CMS-HCC-V28'

)

, unioned as (

    select * from v24_new_enrollees
    union all
    select * from v24_continuining_enrollees
    union all
    select * from v28_new_enrollees
    union all
    select * from v28_continuining_enrollees

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(enrollment_status as {{ dbt.type_string() }}) as enrollment_status
        , cast(gender as {{ dbt.type_string() }}) as gender
        , cast(age_group as {{ dbt.type_string() }}) as age_group
        , cast(medicaid_status as {{ dbt.type_string() }}) as medicaid_status
        , cast(dual_status as {{ dbt.type_string() }}) as dual_status
        , cast(orec as {{ dbt.type_string() }}) as orec
        , cast(institutional_status as {{ dbt.type_string() }}) as institutional_status
        , cast(enrollment_status_default as boolean) as enrollment_status_default
        , cast(medicaid_dual_status_default as boolean) as medicaid_dual_status_default
        , cast(orec_default as boolean) as orec_default
        , cast(institutional_status_default as boolean) as institutional_status_default
        , round(cast(coefficient as {{ dbt.type_numeric() }}),3) as coefficient
        , cast(factor_type as {{ dbt.type_string() }}) as factor_type
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
    from unioned

)

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
    , orec_default
    , institutional_status_default
    , coefficient
    , factor_type
    , model_version
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types