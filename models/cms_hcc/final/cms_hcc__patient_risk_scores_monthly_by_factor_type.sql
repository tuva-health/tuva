{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}
with seed_adjustment_rates as (

    select
          model_version
        , payment_year
        , normalization_factor
        , ma_coding_pattern_adjustment
    from {{ ref('cms_hcc__adjustment_rates') }}

)

, risk_factors as (

    select
          person_id
        , payer
        , factor_type
        , coefficient
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__patient_risk_factors_monthly') }}

)

, member_months as (

    select
          person_id
        , payer
        , cast({{ substring('year_month', 1, 4) }} as integer) as eligible_year
        , count(1) as member_months
    from {{ ref('cms_hcc__stg_core__member_months') }}
    group by
          person_id
        , payer
        , cast({{ substring('year_month', 1, 4) }} as integer)
)

, raw_score as (

    select
          person_id
        , payer
        , factor_type
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
        , sum(coefficient) as risk_score
    from risk_factors
    group by
          person_id
        , payer
        , factor_type
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date

)

/*
    CMS Guidance for the transition from v24 to v28:

    PY2024 risk scores will be blended using 67% of the risk score calculated
    from v24 and 33% from v28.

    PY2025 risk scores will be blended using 33% of the risk score calculated
    from v24 and 67% from v28.

    PY2026 risk scores will be 100% from v28.

    Prior payment years will still be calculated from v24 only.
*/
, transition_scores as (

    select
          raw.person_id
        , raw.payer
        , raw.factor_type
        , raw.risk_score as raw_risk_score
        -- TODO: Uncomment when seed is updated
        -- , raw.risk_score * adj.blend_weight as weighted_raw_risk_score
        , case
            when raw.payment_year <= 2023 and raw.model_version = 'CMS-HCC-V24' then risk_score
            when raw.payment_year = 2024 and raw.model_version = 'CMS-HCC-V24' then risk_score * 0.67
            when raw.payment_year = 2025 and raw.model_version = 'CMS-HCC-V24' then risk_score * 0.33
            when raw.payment_year >= 2026 and raw.model_version = 'CMS-HCC-V24' then 0
            when raw.payment_year <= 2023 and raw.model_version = 'CMS-HCC-V28' then 0
            when raw.payment_year = 2024 and raw.model_version = 'CMS-HCC-V28' then risk_score * 0.33
            when raw.payment_year = 2025 and raw.model_version = 'CMS-HCC-V28' then risk_score * 0.67
            when raw.payment_year >= 2026 and raw.model_version = 'CMS-HCC-V28' then risk_score            
            end as weighted_raw_risk_score
        , adj.normalization_factor
        , adj.ma_coding_pattern_adjustment
        , raw.model_version
        , raw.payment_year
        , raw.collection_start_date
        , raw.collection_end_date
    from raw_score raw
    left outer join seed_adjustment_rates as adj
        on  raw.payment_year = adj.payment_year
        and raw.model_version = adj.model_version

)

, normalized as (

    select
          person_id
        , payer
        , factor_type
        , model_version
        , raw_risk_score
        , weighted_raw_risk_score
        , weighted_raw_risk_score / normalization_factor as normalized_risk_score
        , ma_coding_pattern_adjustment
        , payment_year
        , collection_start_date
        , collection_end_date
    from transition_scores
)

, payment as (

    select
          person_id
        , payer
        , factor_type
        , model_version
        , raw_risk_score
        , weighted_raw_risk_score
        , normalized_risk_score
        , normalized_risk_score * (1 - ma_coding_pattern_adjustment) as payment_risk_score
        , payment_year
        , collection_start_date
        , collection_end_date
    from normalized
)

, blended as (
select
          person_id
        , payer
        , factor_type
        , payment_year
        , collection_start_date
        , collection_end_date        
        , max(case when model_version = 'CMS-HCC-V24' then weighted_raw_risk_score end) as v24_risk_score
        , max(case when model_version = 'CMS-HCC-V28' then weighted_raw_risk_score end) as v28_risk_score
        , sum(weighted_raw_risk_score) as blended_risk_score
        , sum(normalized_risk_score) as normalized_risk_score
        , sum(payment_risk_score) as payment_risk_score
from payment
group by
          person_id
        , payer
        , factor_type
        , payment_year
        , collection_start_date
        , collection_end_date  
)

, weighted_score as (

    select
          blended.person_id
        , blended.payer
        , blended.factor_type
        , blended.v24_risk_score
        , blended.v28_risk_score
        , blended.blended_risk_score
        , blended.normalized_risk_score
        , blended.payment_risk_score
        , member_months.member_months
        , blended.payment_risk_score * member_months.member_months as payment_risk_score_weighted_by_months
        , blended.payment_year
        , blended.collection_start_date
        , blended.collection_end_date
    from blended
    left outer join member_months
            on blended.person_id = member_months.person_id
            and blended.payer = member_months.payer
            and blended.payment_year = member_months.eligible_year
)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(factor_type as {{ dbt.type_string() }}) as factor_type
        , round(cast(v24_risk_score as {{ dbt.type_numeric() }}), 3) as v24_risk_score
        , round(cast(v28_risk_score as {{ dbt.type_numeric() }}), 3) as v28_risk_score
        , round(cast(blended_risk_score as {{ dbt.type_numeric() }}), 3) as blended_risk_score
        , round(cast(normalized_risk_score as {{ dbt.type_numeric() }}), 3) as normalized_risk_score
        , round(cast(payment_risk_score as {{ dbt.type_numeric() }}), 3) as payment_risk_score
        , round(cast(payment_risk_score_weighted_by_months as {{ dbt.type_numeric() }}), 3) as payment_risk_score_weighted_by_months
        , cast(member_months as integer) as member_months
        , cast(payment_year as integer) as payment_year
        , cast(collection_start_date as date) as collection_start_date
        , cast(collection_end_date as date) as collection_end_date
    from weighted_score

)

select
      person_id
    , payer
    , factor_type
    , v24_risk_score
    , v28_risk_score
    , blended_risk_score
    , normalized_risk_score
    , payment_risk_score
    , payment_risk_score_weighted_by_months
    , member_months
    , payment_year
    , collection_start_date
    , collection_end_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
