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

, seed_adjustment_meta as (
    /*
        Clamp payment_year for normalization/MA coding adjustment factors
        to the nearest available year. Use case: allow scoring for payment
        years before CMS publishes the new adjustment table by using the
        earliest if below range and the latest if above.
    */
    select
          min(payment_year) as min_payment_year
        , max(payment_year) as max_payment_year
    from seed_adjustment_rates

)

, risk_factors as (

    select
          person_id
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
        , cast({{ substring('year_month', 1, 4) }} as integer) as eligible_year
        , count(1) as member_months
    from {{ ref('cms_hcc__stg_core__member_months') }}
    group by
        person_id
        , cast({{ substring('year_month', 1, 4) }} as integer)
)

, start as (

    select
          person_id
        , final_start
        , start_source
    from {{ ref('cms_hcc__int_medicare_enrollment_start') }}

)

, patient as (

    select
          person_id
        , death_date
    from {{ ref('cms_hcc__stg_core__patient') }}

)

, raw_score as (

    select
          person_id
        , sum(coefficient) as risk_score
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
    from risk_factors
    group by
          person_id
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
          person_id
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
        , collection_start_date
        , collection_end_date
    from raw_score

)

/*
    Grouping by patient to create a single row per patient.
*/
, transition_scores_grouped as (

    select
          person_id
        , max(v24_risk_score) as v24_risk_score
        , max(v28_risk_score) as v28_risk_score
        , payment_year
        , collection_start_date
        , collection_end_date
    from transition_scores
    group by
          person_id
        , payment_year
        , collection_start_date
        , collection_end_date

)

, blended as (

    select
          person_id
        , v24_risk_score
        , v28_risk_score
        , v24_risk_score + v28_risk_score as blended_risk_score
        , payment_year
        , collection_start_date
        , collection_end_date
    from transition_scores_grouped

)

, normalized as (

    select
          blended.person_id
        , blended.v24_risk_score
        , blended.v28_risk_score
        , blended.blended_risk_score
        , blended.blended_risk_score / seed_adjustment_rates.normalization_factor as normalized_risk_score
        , blended.payment_year
        , blended.collection_start_date
        , blended.collection_end_date
    from blended
        left outer join seed_adjustment_meta on 1=1
        left outer join seed_adjustment_rates
            on seed_adjustment_rates.payment_year = case
                when blended.payment_year < seed_adjustment_meta.min_payment_year then seed_adjustment_meta.min_payment_year
                when blended.payment_year > seed_adjustment_meta.max_payment_year then seed_adjustment_meta.max_payment_year
                else blended.payment_year
            end

)

, payment as (

    select
          normalized.person_id
        , normalized.v24_risk_score
        , normalized.v28_risk_score
        , normalized.blended_risk_score
        , normalized.normalized_risk_score
        , normalized.normalized_risk_score * (1 - seed_adjustment_rates.ma_coding_pattern_adjustment) as payment_risk_score
        , normalized.payment_year
        , normalized.collection_start_date
        , normalized.collection_end_date
    from normalized
        left outer join seed_adjustment_meta on 1=1
        left outer join seed_adjustment_rates
            on seed_adjustment_rates.payment_year = case
                when normalized.payment_year < seed_adjustment_meta.min_payment_year then seed_adjustment_meta.min_payment_year
                when normalized.payment_year > seed_adjustment_meta.max_payment_year then seed_adjustment_meta.max_payment_year
                else normalized.payment_year
            end

)

, weighted_score as (

    select
        payment.person_id
        , payment.v24_risk_score
        , payment.v28_risk_score
        , payment.blended_risk_score
        , payment.normalized_risk_score
        , payment.payment_risk_score
        , member_months.member_months as member_months_core
        , cast({{ concat_custom(['payment.payment_year', "'-01-01'"]) }} as date) as year_start_date
        , cast({{ concat_custom(['payment.payment_year', "'-12-31'"]) }} as date) as year_end_date
        , case when start.final_start is not null and start.final_start > cast({{ concat_custom(['payment.payment_year', "'-01-01'"]) }} as date)
               then start.final_start else cast({{ concat_custom(['payment.payment_year', "'-01-01'"]) }} as date) end as start_in_year
        , case when patient.death_date is not null and patient.death_date < cast({{ concat_custom(['payment.payment_year', "'-12-31'"]) }} as date)
               then patient.death_date else cast({{ concat_custom(['payment.payment_year', "'-12-31'"]) }} as date) end as end_in_year
        , case
            when start.final_start is null then null
            when start.final_start > cast({{ concat_custom(['payment.payment_year', "'-12-31'"]) }} as date) then 0
            else ( {{ datediff('start_in_year', 'end_in_year', 'month') }} + 1 )
          end as member_months_medicare
        , coalesce(member_months.member_months,
                   case
                     when start.final_start is null then null
                     when start.final_start > cast({{ concat_custom(['payment.payment_year', "'-12-31'"]) }} as date) then 0
                     else ( {{ datediff('start_in_year', 'end_in_year', 'month') }} + 1 )
                   end
          ) as member_months
        , case
            when member_months.member_months is not null then 'core_member_months'
            when start.final_start is not null then 'medicare_enrollment'
            else null
          end as member_months_source
        , case when start.start_source = 'inferred_from_claims' and member_months.member_months is null and start.final_start is not null then 1 else 0 end as enrollment_projection_used_int
        , start.start_source
        , payment.payment_risk_score * coalesce(
              member_months.member_months,
              case
                when start.final_start is null then null
                when start.final_start > cast({{ concat_custom(['payment.payment_year', "'-12-31'"]) }} as date) then 0
                else ( {{ datediff('start_in_year', 'end_in_year', 'month') }} + 1 )
              end
          ) as payment_risk_score_weighted_by_months
        , payment.payment_year
        , payment.collection_start_date
        , payment.collection_end_date
    from payment
    left outer join member_months
            on payment.person_id = member_months.person_id
            and payment.payment_year = member_months.eligible_year
    left outer join start
            on payment.person_id = start.person_id
    left outer join patient
            on payment.person_id = patient.person_id
)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , round(cast(v24_risk_score as {{ dbt.type_numeric() }}), 3) as v24_risk_score
        , round(cast(v28_risk_score as {{ dbt.type_numeric() }}), 3) as v28_risk_score
        , round(cast(blended_risk_score as {{ dbt.type_numeric() }}), 3) as blended_risk_score
        , round(cast(normalized_risk_score as {{ dbt.type_numeric() }}), 3) as normalized_risk_score
        , round(cast(payment_risk_score as {{ dbt.type_numeric() }}), 3) as payment_risk_score
        , round(cast(payment_risk_score_weighted_by_months as {{ dbt.type_numeric() }}), 3) as payment_risk_score_weighted_by_months
        , cast(member_months as integer) as member_months
        , cast(member_months_core as integer) as member_months_core
        , cast(member_months_medicare as integer) as member_months_medicare
        , cast(member_months_source as {{ dbt.type_string() }}) as member_months_source
        {% if target.type == 'fabric' %}
            , cast(enrollment_projection_used_int as bit) as enrollment_projection_used
        {% else %}
            , cast(case when enrollment_projection_used_int = 1 then true else false end as boolean) as enrollment_projection_used
        {% endif %}
        , cast(start_source as {{ dbt.type_string() }}) as start_source
        , cast(payment_year as integer) as payment_year
        , cast(collection_start_date as date) as collection_start_date
        , cast(collection_end_date as date) as collection_end_date
    from weighted_score

)

select
      person_id
    , v24_risk_score
    , v28_risk_score
    , blended_risk_score
    , normalized_risk_score
    , payment_risk_score
    , payment_risk_score_weighted_by_months
    , member_months
    , member_months_core
    , member_months_medicare
    , member_months_source
    , enrollment_projection_used
    , start_source
    , payment_year
    , collection_start_date
    , collection_end_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
