{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
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
          patient_id
        , coefficient
        , model_version
        , payment_year
    from {{ ref('cms_hcc__patient_risk_factors') }}

)

, raw_score as (

    select
          patient_id
        , sum(coefficient) as risk_score
        , model_version
        , payment_year
    from risk_factors
    group by
          patient_id
        , model_version
        , payment_year

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
          patient_id
        , risk_score
        , case
            when payment_year <= 2023 and model_version = 'CMS-HCC-V24' then risk_score
            when payment_year = 2024 and model_version = 'CMS-HCC-V24' then risk_score * 0.67
            when payment_year = 2025 and model_version = 'CMS-HCC-V24' then risk_score * 0.33
            when payment_year >= 2026 and model_version = 'CMS-HCC-V24' then 0
            end as v24_risk_score
        , case
            when payment_year <= 2023 and model_version = 'CMS-HCC-V28' then 0
            when payment_year = 2024 and model_version = 'CMS-HCC-V28' then risk_score * 0.33
            when payment_year = 2025 and model_version = 'CMS-HCC-V28' then risk_score * 0.67
            when payment_year >= 2026 and model_version = 'CMS-HCC-V28' then risk_score
            end as v28_risk_score
        , model_version
        , payment_year
    from raw_score

)

/*
    Grouping by patient to create a single row per patient.
*/
, transition_scores_grouped as (

    select
          patient_id
        , max(v24_risk_score) as v24_risk_score
        , max(v28_risk_score) as v28_risk_score
        , payment_year
    from transition_scores
    group by
          patient_id
        , payment_year

)

, blended as (

    select
          patient_id
        , v24_risk_score
        , v28_risk_score
        , v24_risk_score + v28_risk_score as blended_risk_score
        , payment_year
    from transition_scores_grouped

)

, normalized as (

    select
          blended.patient_id
        , blended.v24_risk_score
        , blended.v28_risk_score
        , blended.blended_risk_score
        , blended.blended_risk_score / seed_adjustment_rates.normalization_factor as normalized_risk_score
        , blended.payment_year
    from blended
        left join seed_adjustment_rates
            on blended.payment_year = seed_adjustment_rates.payment_year

)

, payment as (

    select
          normalized.patient_id
        , normalized.v24_risk_score
        , normalized.v28_risk_score
        , normalized.blended_risk_score
        , normalized.normalized_risk_score
        , normalized.normalized_risk_score * (1 - seed_adjustment_rates.ma_coding_pattern_adjustment) as payment_risk_score
        , normalized.payment_year
    from normalized
        left join seed_adjustment_rates
            on normalized.payment_year = seed_adjustment_rates.payment_year

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , round(cast(v24_risk_score as {{ dbt.type_numeric() }}),3) as v24_risk_score
        , round(cast(v28_risk_score as {{ dbt.type_numeric() }}),3) as v28_risk_score
        , round(cast(blended_risk_score as {{ dbt.type_numeric() }}),3) as blended_risk_score
        , round(cast(normalized_risk_score as {{ dbt.type_numeric() }}),3) as normalized_risk_score
        , round(cast(payment_risk_score as {{ dbt.type_numeric() }}),3) as payment_risk_score
        , cast(payment_year as integer) as payment_year
    from payment

)

select
      patient_id
    , v24_risk_score
    , v28_risk_score
    , blended_risk_score
    , normalized_risk_score
    , payment_risk_score
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types