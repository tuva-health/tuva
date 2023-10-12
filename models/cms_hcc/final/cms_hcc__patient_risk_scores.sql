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
        , round(cast(sum(coefficient) as {{ dbt.type_numeric() }}),3) as risk_score
        , model_version
        , payment_year
    from risk_factors
    group by
          patient_id
        , model_version
        , payment_year

)

, normalized as (

    select
          raw_score.patient_id
        , raw_score.risk_score as raw_risk_score
        , round(cast(raw_score.risk_score / seed_adjustment_rates.normalization_factor as {{ dbt.type_numeric() }}),3) as normalized_risk_score
        , raw_score.model_version
        , raw_score.payment_year
    from raw_score
         left join seed_adjustment_rates
         on raw_score.payment_year = seed_adjustment_rates.payment_year
         and raw_score.model_version = seed_adjustment_rates.model_version

)

, payment as (

    select
          normalized.patient_id
        , normalized.raw_risk_score
        , normalized.normalized_risk_score
        , round(cast(normalized.normalized_risk_score * (1 - seed_adjustment_rates.ma_coding_pattern_adjustment) as {{ dbt.type_numeric() }}),3) as payment_risk_score
        , normalized.model_version
        , normalized.payment_year
    from normalized
         left join seed_adjustment_rates
         on normalized.payment_year = seed_adjustment_rates.payment_year
         and normalized.model_version = seed_adjustment_rates.model_version

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , round(cast(raw_risk_score as {{ dbt.type_numeric() }}),3) as raw_risk_score
        , round(cast(normalized_risk_score as {{ dbt.type_numeric() }}),3) as normalized_risk_score
        , round(cast(payment_risk_score as {{ dbt.type_numeric() }}),3) as payment_risk_score
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
    from payment

)

select
      patient_id
    , raw_risk_score
    , normalized_risk_score
    , payment_risk_score
    , model_version
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types