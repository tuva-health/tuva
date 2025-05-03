{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with demographics as (

    select
          person_id
        , enrollment_status
        , gender
        , age_group
        , medicaid_status
        , dual_status
        , orec
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
        , medicaid_status
        , dual_status
        , orec
        , institutional_status
        , short_name
        , description
        , hcc_code_1
        , hcc_code_2
        , coefficient
    from {{ ref('cms_hcc__disease_interaction_factors') }}

)

, demographics_with_hccs as (

    select
          demographics.person_id
        , demographics.enrollment_status
        , demographics.medicaid_status
        , demographics.dual_status
        , demographics.orec
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

, demographics_with_interactions as (

    select
          demographics_with_hccs.person_id
        , demographics_with_hccs.model_version
        , demographics_with_hccs.payment_year
        , demographics_with_hccs.collection_start_date
        , demographics_with_hccs.collection_end_date
        , interactions_code_1.factor_type
        , interactions_code_1.description
        , interactions_code_1.hcc_code_1
        , interactions_code_1.hcc_code_2
        , interactions_code_1.coefficient
    from demographics_with_hccs
        inner join seed_interaction_factors as interactions_code_1
            on demographics_with_hccs.enrollment_status = interactions_code_1.enrollment_status
            and demographics_with_hccs.medicaid_status = interactions_code_1.medicaid_status
            and demographics_with_hccs.dual_status = interactions_code_1.dual_status
            and demographics_with_hccs.orec = interactions_code_1.orec
            and demographics_with_hccs.institutional_status = interactions_code_1.institutional_status
            and demographics_with_hccs.hcc_code = interactions_code_1.hcc_code_1
            and demographics_with_hccs.model_version = interactions_code_1.model_version

)

, disease_interactions as (

    select
          demographics_with_interactions.person_id
        , demographics_with_interactions.factor_type
        , demographics_with_interactions.hcc_code_1
        , demographics_with_interactions.hcc_code_2
        , demographics_with_interactions.description
        , demographics_with_interactions.coefficient
        , demographics_with_interactions.model_version
        , demographics_with_interactions.payment_year
        , demographics_with_interactions.collection_start_date
        , demographics_with_interactions.collection_end_date
    from demographics_with_interactions
        inner join demographics_with_hccs as interactions_code_2
            on demographics_with_interactions.person_id = interactions_code_2.person_id
            and demographics_with_interactions.hcc_code_2 = interactions_code_2.hcc_code
            and demographics_with_interactions.model_version = interactions_code_2.model_version
            and demographics_with_interactions.payment_year = interactions_code_2.payment_year
            and demographics_with_interactions.collection_end_date = interactions_code_2.collection_end_date
)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(hcc_code_1 as {{ dbt.type_string() }}) as hcc_code_1
        , cast(hcc_code_2 as {{ dbt.type_string() }}) as hcc_code_2
        , cast(description as {{ dbt.type_string() }}) as description
        , round(cast(coefficient as {{ dbt.type_numeric() }}), 3) as coefficient
        , cast(factor_type as {{ dbt.type_string() }}) as factor_type
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
        , cast(collection_start_date as date) as collection_start_date
        , cast(collection_end_date as date) as collection_end_date
    from disease_interactions

)

select
      person_id
    , hcc_code_1
    , hcc_code_2
    , description
    , coefficient
    , factor_type
    , model_version
    , payment_year
    , collection_start_date
    , collection_end_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
