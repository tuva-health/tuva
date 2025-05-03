{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with demographics as (

    select
          person_id
        , enrollment_status
        , institutional_status
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__int_demographic_factors') }}

)

, hcc_hierarchy as (

    select
          person_id
        , hcc_code
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__int_hcc_hierarchy') }}

)

, seed_interaction_factors as (

    select
          model_version
        , factor_type
        , enrollment_status
        , institutional_status
        , short_name
        , description
        , hcc_code
        , coefficient
    from {{ ref('cms_hcc__disabled_interaction_factors') }}

)

, demographics_with_hccs as (

    select
          demographics.person_id
        , demographics.enrollment_status
        , demographics.institutional_status
        , demographics.model_version
        , demographics.payment_year
        , demographics.collection_start_date
        , demographics.collection_end_date
        , hcc_hierarchy.hcc_code
    from demographics
        inner join hcc_hierarchy
            on demographics.person_id = hcc_hierarchy.person_id
            and demographics.model_version = hcc_hierarchy.model_version
            and demographics.payment_year = hcc_hierarchy.payment_year
            and demographics.collection_end_date = hcc_hierarchy.collection_end_date

)

, interactions as (

    select
          demographics_with_hccs.person_id
        , demographics_with_hccs.model_version
        , demographics_with_hccs.payment_year
        , demographics_with_hccs.collection_start_date
        , demographics_with_hccs.collection_end_date
        , seed_interaction_factors.factor_type
        , seed_interaction_factors.description
        , seed_interaction_factors.coefficient
    from demographics_with_hccs
        inner join seed_interaction_factors
            on demographics_with_hccs.enrollment_status = seed_interaction_factors.enrollment_status
            and demographics_with_hccs.institutional_status = seed_interaction_factors.institutional_status
            and demographics_with_hccs.hcc_code = seed_interaction_factors.hcc_code
            and demographics_with_hccs.model_version = seed_interaction_factors.model_version

)

, add_data_types as (

select
      cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(description as {{ dbt.type_string() }}) as description
    , round(cast(coefficient as {{ dbt.type_numeric() }}), 3) as coefficient
    , cast(factor_type as {{ dbt.type_string() }}) as factor_type
    , cast(model_version as {{ dbt.type_string() }}) as model_version
    , cast(payment_year as integer) as payment_year
    , cast(collection_start_date as date) as collection_start_date
    , cast(collection_end_date as date) as collection_end_date
from interactions

)

select
      person_id
    , description
    , coefficient
    , factor_type
    , model_version
    , payment_year
    , collection_start_date
    , collection_end_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
