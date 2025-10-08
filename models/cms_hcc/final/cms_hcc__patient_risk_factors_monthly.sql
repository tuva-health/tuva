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
        , cast(null as {{ dbt.type_string() }}) as hcc_code
        , cast(null as {{ dbt.type_string() }}) as hcc_code_1
        , cast(null as {{ dbt.type_string() }}) as hcc_code_2
        , cast(null as {{ dbt.type_string() }}) as payment_hcc_count

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
        , eligibility_imputed
        , payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__int_demographic_factors') }}

)

, demographic_lookup as (

    select
          person_id
        , model_version
        , payment_year
        , collection_end_date
        , enrollment_status
        , gender
        , age_group
        , medicaid_status
        , dual_status
        , orec
        , institutional_status
    from {{ ref('cms_hcc__int_demographic_factors') }}

)

, disease_factors as (

    select
          person_id
        , {{ concat_custom(["hcc_description", "' (HCC '", "hcc_code", "')'"]) }} as description
        , hcc_code
        , cast(null as {{ dbt.type_string() }}) as hcc_code_1
        , cast(null as {{ dbt.type_string() }}) as hcc_code_2
        , cast(null as {{ dbt.type_string() }}) as payment_hcc_count
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
        , cast(null as {{ dbt.type_string() }}) as hcc_code
        , cast(null as {{ dbt.type_string() }}) as hcc_code_1
        , cast(null as {{ dbt.type_string() }}) as hcc_code_2
        , cast(null as {{ dbt.type_string() }}) as payment_hcc_count
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
        , hcc_code
        , cast(null as {{ dbt.type_string() }}) as hcc_code_1
        , cast(null as {{ dbt.type_string() }}) as hcc_code_2
        , cast(null as {{ dbt.type_string() }}) as payment_hcc_count
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
        , cast(null as {{ dbt.type_string() }}) as hcc_code
        , hcc_code_1
        , hcc_code_2
        , cast(null as {{ dbt.type_string() }}) as payment_hcc_count
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
        , cast(null as {{ dbt.type_string() }}) as hcc_code
        , cast(null as {{ dbt.type_string() }}) as hcc_code_1
        , cast(null as {{ dbt.type_string() }}) as hcc_code_2
        , payment_hcc_count
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
        , {{ cms_hcc_demographic_key(
              unioned.model_version,
              demographic_lookup.enrollment_status,
              demographic_lookup.gender,
              demographic_lookup.age_group,
              demographic_lookup.medicaid_status,
              demographic_lookup.dual_status,
              demographic_lookup.orec,
              demographic_lookup.institutional_status
          ) }} as demographic_key
        , demographic_lookup.enrollment_status as demographic_enrollment_status
        , demographic_lookup.gender as demographic_gender
        , demographic_lookup.age_group as demographic_age_group
        , demographic_lookup.medicaid_status as demographic_medicaid_status
        , demographic_lookup.dual_status as demographic_dual_status
        , demographic_lookup.orec as demographic_orec
        , demographic_lookup.institutional_status as demographic_institutional_status
        , case
            when unioned.factor_type = 'Demographic' then {{ concat_custom([
                  "'DEM|'",
                  cms_hcc_demographic_key(
                  unioned.model_version,
                  demographic_lookup.enrollment_status,
                  demographic_lookup.gender,
                  demographic_lookup.age_group,
                  demographic_lookup.medicaid_status,
                  demographic_lookup.dual_status,
                  demographic_lookup.orec,
                  demographic_lookup.institutional_status
                  )
              ]) }}
            when unioned.hcc_code_1 is not null then {{ concat_custom([
                  "unioned.model_version",
                  "'|HCC1:'",
                  "unioned.hcc_code_1",
                  "'|HCC2:'",
                  "unioned.hcc_code_2",
                  "'|'",
                  "demographic_lookup.enrollment_status",
                  "'|'",
                  "demographic_lookup.medicaid_status",
                  "'|'",
                  "demographic_lookup.dual_status",
                  "'|'",
                  "demographic_lookup.orec",
                  "'|'",
                  "demographic_lookup.institutional_status"
              ]) }}
            when unioned.factor_type = 'Disease' then {{ concat_custom([
                  "unioned.model_version",
                  "'|HCC:'",
                  "unioned.hcc_code",
                  "'|'",
                  "demographic_lookup.enrollment_status",
                  "'|'",
                  "demographic_lookup.medicaid_status",
                  "'|'",
                  "demographic_lookup.dual_status",
                  "'|'",
                  "demographic_lookup.orec",
                  "'|'",
                  "demographic_lookup.institutional_status"
              ]) }}
            when unioned.hcc_code is not null then {{ concat_custom([
                  "unioned.model_version",
                  "'|HCC:'",
                  "unioned.hcc_code",
                  "'|'",
                  "demographic_lookup.enrollment_status",
                  "'|'",
                  "demographic_lookup.institutional_status"
              ]) }}
            when unioned.payment_hcc_count is not null then {{ concat_custom([
                  "unioned.model_version",
                  "'|'",
                  "demographic_lookup.enrollment_status",
                  "'|'",
                  "demographic_lookup.medicaid_status",
                  "'|'",
                  "demographic_lookup.dual_status",
                  "'|'",
                  "demographic_lookup.orec",
                  "'|'",
                  "demographic_lookup.institutional_status",
                  "'|COUNT:'",
                  "unioned.payment_hcc_count"
              ]) }}
            else {{ concat_custom([
                   "unioned.model_version",
                   "'|'",
                   "demographic_lookup.gender",
                   "'|'",
                   "demographic_lookup.enrollment_status",
                   "'|'",
                   "demographic_lookup.medicaid_status",
                   "'|'",
                   "demographic_lookup.dual_status",
                   "'|'",
                   "demographic_lookup.institutional_status"
               ]) }}
          end as risk_factor_key
        , unioned.coefficient
        , unioned.factor_type
        , unioned.model_version
        , unioned.payment_year
        , unioned.collection_start_date
        , unioned.collection_end_date
        , demographic_defaults.eligibility_imputed
    from unioned
        left outer join demographic_defaults
            on unioned.person_id = demographic_defaults.person_id
            and unioned.model_version = demographic_defaults.model_version
            and unioned.payment_year = demographic_defaults.payment_year
            and unioned.collection_end_date = demographic_defaults.collection_end_date
        left outer join demographic_lookup
            on unioned.person_id = demographic_lookup.person_id
            and unioned.model_version = demographic_lookup.model_version
            and unioned.payment_year = demographic_lookup.payment_year
            and unioned.collection_end_date = demographic_lookup.collection_end_date

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(demographic_key as {{ dbt.type_string() }}) as demographic_key
        , cast(demographic_enrollment_status as {{ dbt.type_string() }}) as demographic_enrollment_status
        , cast(demographic_gender as {{ dbt.type_string() }}) as demographic_gender
        , cast(demographic_age_group as {{ dbt.type_string() }}) as demographic_age_group
        , cast(demographic_medicaid_status as {{ dbt.type_string() }}) as demographic_medicaid_status
        , cast(demographic_dual_status as {{ dbt.type_string() }}) as demographic_dual_status
        , cast(demographic_orec as {{ dbt.type_string() }}) as demographic_orec
        , cast(demographic_institutional_status as {{ dbt.type_string() }}) as demographic_institutional_status
        {% if target.type == 'fabric' %}
            , cast(enrollment_status_default as bit) as enrollment_status_default
            , cast(medicaid_dual_status_default as bit) as medicaid_dual_status_default
            , cast(orec_default as bit) as orec_default
            , cast(institutional_status_default as bit) as institutional_status_default
            , cast(eligibility_imputed as bit) as eligibility_imputed
        {% else %}
            , cast(enrollment_status_default as boolean) as enrollment_status_default
            , cast(medicaid_dual_status_default as boolean) as medicaid_dual_status_default
            , cast(orec_default as boolean) as orec_default
            , cast(institutional_status_default as boolean) as institutional_status_default
            , cast(eligibility_imputed as boolean) as eligibility_imputed
        {% endif %}
        , cast(factor_type as {{ dbt.type_string() }}) as factor_type
        , cast(risk_factor_key as {{ dbt.type_string() }}) as risk_factor_key
        , round(cast(coefficient as {{ dbt.type_numeric() }}), 3) as coefficient
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
        , cast(collection_start_date as date) as collection_start_date
        , cast(collection_end_date as date) as collection_end_date
    from add_defaults

)

select
      person_id
    , demographic_key
    , demographic_enrollment_status
    , demographic_gender
    , demographic_age_group
    , demographic_medicaid_status
    , demographic_dual_status
    , demographic_orec
    , demographic_institutional_status
    , enrollment_status_default
    , medicaid_dual_status_default
    , orec_default
    , institutional_status_default
    , factor_type
    , risk_factor_key
    , coefficient
    , model_version
    , payment_year
    , collection_start_date
    , collection_end_date
    , eligibility_imputed
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
