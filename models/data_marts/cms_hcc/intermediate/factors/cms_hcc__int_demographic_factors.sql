{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with members as (

    select
          person_id
        , payer
        , enrollment_status
        , gender
        , age_group
        , medicaid_status
        , dual_status
        , orec
        , originally_disabled_flag
        , institutional_status
        , institutional_snp_flag
        , enrollment_status_default
        , medicaid_dual_status_default
        , orec_default
        , institutional_status_default
        , payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__int_members') }}

)

, seed_demographic_factors as (
    select
          model_version
        , factor_type
        , case when institutional_status = 'Yes' then 'Institutional' else enrollment_status end as enrollment_status
        , gender
        , age_group
        , medicaid_status
        , dual_status
        , orec
        , institutional_status
        , coefficient
        , case
            -- ESRD
            when enrollment_status = 'ESRD' then 'ESRD'
            -- New Enrollee
            when enrollment_status = 'New' then 'E'
            -- Long Term Institutional (INS)
            when institutional_status = 'Yes' then 'INS'
            -- Community NonDual Aged (CNA)
            when medicaid_status = 'No' and orec = 'Aged' then 'CNA'
            -- Community NonDual Disabled (CND)
            when medicaid_status = 'No' and orec = 'Disabled' then 'CND'
            -- Community Full Benefit Dual Aged (CFA)
            when dual_status = 'Full' and orec = 'Aged' then 'CFA'
            -- Community Full Benefit Dual Disabled (CFD)
            when dual_status = 'Full' and orec = 'Disabled' then 'CFD'
            -- Community Partial Benefit Dual Aged (CPA)
            when dual_status = 'Partial' and orec = 'Aged' then 'CPA'
            -- Community Partial Benefit Dual Disabled (CPD)
            when dual_status = 'Partial' and orec = 'Disabled' then 'CPD'
        end as risk_model_code
    from {{ ref('cms_hcc__demographic_factors') }}
    where plan_segment is null

)

/*
    SNPNE coefficients are stored with plan_segment = 'C-SNP' in the seed.
    These are distinct from regular NE coefficients and apply to members
    enrolled in a Special Needs Plan.
*/
, seed_snpne_factors as (
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
        , cast('SNPNE' as {{ dbt.type_string() }}) as risk_model_code
    from {{ ref('cms_hcc__demographic_factors') }}
    where plan_segment = 'C-SNP'

)

/*
    NE seed uses orec to distinguish ORIGDIS vs NORIGDIS:
      - 'Disabled' in seed = originally disabled (OREC=1, age 65+)
      - 'Aged' in seed = not originally disabled (everyone else)
*/
, new_enrollees as (
    select
          members.person_id
        , members.payer
        , members.enrollment_status
        , members.gender
        , members.age_group
        , members.medicaid_status
        , members.dual_status
        , members.orec
        , members.originally_disabled_flag
        , members.institutional_status
        , members.enrollment_status_default
        , members.medicaid_dual_status_default
        , members.orec_default
        , members.institutional_status_default
        , members.payment_year
        , members.collection_start_date
        , members.collection_end_date
        , seed_demographic_factors.model_version
        , seed_demographic_factors.factor_type
        , seed_demographic_factors.coefficient
        , seed_demographic_factors.risk_model_code
    from members
    inner join seed_demographic_factors
        on members.enrollment_status = seed_demographic_factors.enrollment_status
        and members.gender = seed_demographic_factors.gender
        and members.age_group = seed_demographic_factors.age_group
        and members.medicaid_status = seed_demographic_factors.medicaid_status
        and case
                when members.originally_disabled_flag = 'Yes' then 'Disabled'
                else 'Aged'
            end = seed_demographic_factors.orec
    where members.enrollment_status = 'New'
        and coalesce(members.institutional_snp_flag, 0) != 1
)

/*
    SNP New Enrollees use SNPNE-specific coefficients (plan_segment = 'C-SNP'
    in the seed). Same join structure as regular NE but for members enrolled
    in a Special Needs Plan.
*/
, snpne_enrollees as (
    select
          members.person_id
        , members.payer
        , members.enrollment_status
        , members.gender
        , members.age_group
        , members.medicaid_status
        , members.dual_status
        , members.orec
        , members.originally_disabled_flag
        , members.institutional_status
        , members.enrollment_status_default
        , members.medicaid_dual_status_default
        , members.orec_default
        , members.institutional_status_default
        , members.payment_year
        , members.collection_start_date
        , members.collection_end_date
        , seed_snpne_factors.model_version
        , seed_snpne_factors.factor_type
        , seed_snpne_factors.coefficient
        , seed_snpne_factors.risk_model_code
    from members
    inner join seed_snpne_factors
        on members.gender = seed_snpne_factors.gender
        and members.age_group = seed_snpne_factors.age_group
        and members.medicaid_status = seed_snpne_factors.medicaid_status
        and case
                when members.originally_disabled_flag = 'Yes' then 'Disabled'
                else 'Aged'
            end = seed_snpne_factors.orec
    where members.enrollment_status = 'New'
        and members.institutional_snp_flag = 1
)

, continuing_enrollees as (
    select
          members.person_id
        , members.payer
        , members.enrollment_status
        , members.gender
        , members.age_group
        , members.medicaid_status
        , members.dual_status
        , members.orec
        , members.originally_disabled_flag
        , members.institutional_status
        , members.enrollment_status_default
        , members.medicaid_dual_status_default
        , members.orec_default
        , members.institutional_status_default
        , members.payment_year
        , members.collection_start_date
        , members.collection_end_date
        , seed_demographic_factors.model_version
        , seed_demographic_factors.factor_type
        , seed_demographic_factors.coefficient
        , seed_demographic_factors.risk_model_code
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
)


, institutional_enrollees as (
    select
          members.person_id
        , members.payer
        , members.enrollment_status
        , members.gender
        , members.age_group
        , members.medicaid_status
        , members.dual_status
        , members.orec
        , members.originally_disabled_flag
        , members.institutional_status
        , members.enrollment_status_default
        , members.medicaid_dual_status_default
        , members.orec_default
        , members.institutional_status_default
        , members.payment_year
        , members.collection_start_date
        , members.collection_end_date
        , seed_demographic_factors.model_version
        , seed_demographic_factors.factor_type
        , seed_demographic_factors.coefficient
        , seed_demographic_factors.risk_model_code
    from members
    inner join seed_demographic_factors
        on members.enrollment_status = seed_demographic_factors.enrollment_status
        and members.gender = seed_demographic_factors.gender
        and members.age_group = seed_demographic_factors.age_group
    where members.enrollment_status = 'Institutional'
)

, unioned as (
    select * from new_enrollees
    union all
    select * from snpne_enrollees
    union all
    select * from continuing_enrollees
    union all
    select * from institutional_enrollees
)

, add_data_types as (
    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(enrollment_status as {{ dbt.type_string() }}) as enrollment_status
        , cast(gender as {{ dbt.type_string() }}) as gender
        , cast(age_group as {{ dbt.type_string() }}) as age_group
        , cast(medicaid_status as {{ dbt.type_string() }}) as medicaid_status
        , cast(dual_status as {{ dbt.type_string() }}) as dual_status
        , cast(orec as {{ dbt.type_string() }}) as orec
        , cast(institutional_status as {{ dbt.type_string() }}) as institutional_status
        , cast(originally_disabled_flag as {{ dbt.type_string() }}) as originally_disabled_flag
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
        , round(cast(coefficient as {{ dbt.type_numeric() }}), 3) as coefficient
        , cast(risk_model_code as {{ dbt.type_string() }}) as risk_model_code
        , cast(factor_type as {{ dbt.type_string() }}) as factor_type
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
        , cast(collection_start_date as date) as collection_start_date
        , cast(collection_end_date as date) as collection_end_date
    from unioned
)

select
      person_id
    , payer
    , enrollment_status
    , gender
    , age_group
    , medicaid_status
    , dual_status
    , orec
    , institutional_status
    , originally_disabled_flag
    , enrollment_status_default
    , medicaid_dual_status_default
    , orec_default
    , institutional_status_default
    , coefficient
    , factor_type
    , risk_model_code
    , model_version
    , payment_year
    , collection_start_date
    , collection_end_date
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from add_data_types
-- 100% v28 starting in 2026
-- TODO: Update the seed table so this filter isn't necessary
where 1 = (case when payment_year >= 2026 and model_version = 'CMS-HCC-V24' then 0 else 1 end)
