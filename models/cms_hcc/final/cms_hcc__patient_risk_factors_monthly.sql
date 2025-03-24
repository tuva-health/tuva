{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}
with demographic_factors as (

    select
          person_id
        /* concatenate demographic risk factors */

    , {{ dbt.concat(
        [
            "gender",
            "', '",
            "age_group",
            "' Years'",
            "', '",
            "enrollment_status",
            "' Enrollee'",
            "', '",
            "CASE"
            "   WHEN medicaid_status = 'Yes' THEN 'Medicaid'"
            "   ELSE 'Non-Medicaid'"
            " END",
            "', '",
            "dual_status",
            "' Dual'",
            "', '",
            "orec",
            "', '",
            "CASE"
            "   WHEN institutional_status = 'Yes' THEN 'Institutional'"
            "   ELSE 'Non-Institutional'"
            " END"
        ]
    ) }} as description

        , coefficient
        , factor_type
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__int_demographic_factors') }}

)

, demographic_defaults as (

    select
          person_id
        , model_version
        , enrollment_status_default
        , medicaid_dual_status_default
        , orec_default
        , institutional_status_default
        , payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__int_demographic_factors') }}

)

, disease_factors as (

    select
          person_id
        , {{ concat_custom(["hcc_description", "' (HCC '", "hcc_code", "')'"]) }} as description
        , coefficient
        , factor_type
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__int_disease_factors') }}

)

, enrollment_interactions as (

    select
          person_id
        , description
        , coefficient
        , factor_type
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__int_enrollment_interaction_factors') }}

)

, disabled_interactions as (

    select
          person_id
        , description
        , coefficient
        , factor_type
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__int_disabled_interaction_factors') }}

)

, disease_interactions as (

    select
          person_id
        , description
        , coefficient
        , factor_type
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__int_disease_interaction_factors') }}

)

, hcc_counts as (

    select
          person_id
        , description
        , coefficient
        , factor_type
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
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
          unioned.person_id
        , demographic_defaults.enrollment_status_default
        , demographic_defaults.medicaid_dual_status_default
        , demographic_defaults.orec_default
        , demographic_defaults.institutional_status_default
        , unioned.description as risk_factor_description
        , unioned.coefficient
        , unioned.factor_type
        , unioned.model_version
        , unioned.payment_year
        , unioned.collection_start_date
        , unioned.collection_end_date
    from unioned
        left join demographic_defaults
            on unioned.person_id = demographic_defaults.person_id
            and unioned.model_version = demographic_defaults.model_version
            and unioned.payment_year = demographic_defaults.payment_year
            and unioned.collection_end_date = demographic_defaults.collection_end_date

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        {% if target.type == 'fabric' %}
            , cast(enrollment_status_default as bit) as enrollment_status_default
            , cast(medicaid_dual_status_default as bit) as medicaid_dual_status_default
            , cast(orec_default as bit) as orec_default
            , cast(institutional_status_default as bit) as institutional_status_default
        {% else %}
            , cast(enrollment_status_default as boolean) as enrollment_status_default
            , cast(medicaid_dual_status_default as boolean) as medicaid_dual_status_default
            , cast(orec_default as boolean) as orec_default
            , cast(institutional_status_default as boolean) as institutional_status_default
        {% endif %}
        , cast(factor_type as {{ dbt.type_string() }}) as factor_type
        , cast(risk_factor_description as {{ dbt.type_string() }}) as risk_factor_description
        , round(cast(coefficient as {{ dbt.type_numeric() }}),3) as coefficient
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
        , cast(collection_start_date as date) as collection_start_date
        , cast(collection_end_date as date) as collection_end_date
    from add_defaults

)

select
      person_id
    , enrollment_status_default
    , medicaid_dual_status_default
    , orec_default
    , institutional_status_default
    , factor_type
    , risk_factor_description
    , coefficient
    , model_version
    , payment_year
    , collection_start_date
    , collection_end_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types