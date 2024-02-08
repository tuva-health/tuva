{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with demographics as (

    select
          patient_id
        , enrollment_status
        , medicaid_status
        , dual_status
        , orec
        , institutional_status
        , model_version
        , payment_year
    from {{ ref('cms_hcc__int_demographic_factors') }}

)

, seed_payment_hcc_count_factors as (

    select
          model_version
        , factor_type
        , enrollment_status
        , medicaid_status
        , dual_status
        , orec
        , institutional_status
        , payment_hcc_count
        , description
        , coefficient
    from {{ ref('cms_hcc__payment_hcc_count_factors') }}

)

, hcc_hierarchy as (

    select
          patient_id
        , hcc_code
        , model_version
    from {{ ref('cms_hcc__int_hcc_hierarchy') }}

)

, demographics_with_hcc_counts as (

    select
          demographics.patient_id
        , demographics.enrollment_status
        , demographics.medicaid_status
        , demographics.dual_status
        , demographics.orec
        , demographics.institutional_status
        , demographics.model_version
        , demographics.payment_year
        , count(hcc_hierarchy.hcc_code) as hcc_count
    from demographics
        inner join hcc_hierarchy
            on demographics.patient_id = hcc_hierarchy.patient_id
            and demographics.model_version = hcc_hierarchy.model_version
    group by
          demographics.patient_id
        , demographics.enrollment_status
        , demographics.medicaid_status
        , demographics.dual_status
        , demographics.orec
        , demographics.institutional_status
        , demographics.model_version
        , demographics.payment_year

)

, hcc_counts_normalized as (

    select
          patient_id
        , enrollment_status
        , medicaid_status
        , dual_status
        , orec
        , institutional_status
        , model_version
        , payment_year
        , case
            when hcc_count >= 10 then '>=10'
            else cast(hcc_count as {{ dbt.type_string() }})
          end as hcc_count_string
    from demographics_with_hcc_counts

)

, hcc_counts as (

    select
          hcc_counts_normalized.patient_id
        , hcc_counts_normalized.model_version
        , hcc_counts_normalized.payment_year
        , seed_payment_hcc_count_factors.factor_type
        , seed_payment_hcc_count_factors.description
        , seed_payment_hcc_count_factors.coefficient
    from hcc_counts_normalized
        inner join seed_payment_hcc_count_factors
            on hcc_counts_normalized.enrollment_status = seed_payment_hcc_count_factors.enrollment_status
            and hcc_counts_normalized.medicaid_status = seed_payment_hcc_count_factors.medicaid_status
            and hcc_counts_normalized.dual_status = seed_payment_hcc_count_factors.dual_status
            and hcc_counts_normalized.orec = seed_payment_hcc_count_factors.orec
            and hcc_counts_normalized.institutional_status = seed_payment_hcc_count_factors.institutional_status
            and hcc_counts_normalized.hcc_count_string = seed_payment_hcc_count_factors.payment_hcc_count
            and hcc_counts_normalized.model_version = seed_payment_hcc_count_factors.model_version

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(description as {{ dbt.type_string() }}) as description
        , round(cast(coefficient as {{ dbt.type_numeric() }}),3) as coefficient
        , cast(factor_type as {{ dbt.type_string() }}) as factor_type
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
    from hcc_counts

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