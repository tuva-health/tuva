{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with member_months as (
    select
        year_month
      , count(1) as member_months
    from {{ ref('core__member_months') }}
    group by year_month
)

, encounters as (
    select
        cast(cal.year_month_int as {{ dbt.type_string() }}) as year_month
      , e.encounter_group
      , e.encounter_type
      , e.encounter_id
      , e.paid_amount
    from {{ ref('core__encounter') }} as e
    left join {{ ref('reference_data__calendar') }} as cal
      on e.encounter_start_date = cal.full_date
)

, pkpy_trend as (
    select
        enc.year_month
      , enc.encounter_group
      , enc.encounter_type
      , count(enc.encounter_id) / mm.member_months * 12000 as pkpy
      , sum(enc.paid_amount) / nullif(count(enc.encounter_id), 0) as paid_per
    from encounters as enc
    inner join member_months as mm
      on enc.year_month = mm.year_month
    group by
        enc.year_month
      , enc.encounter_group
      , enc.encounter_type
      , mm.member_months
)

select
    pkpy_trend.year_month
  , pkpy_trend.encounter_group
  , pkpy_trend.encounter_type
  , pkpy_trend.pkpy
  , pkpy_trend.paid_per
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from pkpy_trend

