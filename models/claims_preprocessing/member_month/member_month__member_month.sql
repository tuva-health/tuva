{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

with stg_eligibility as (
  select
    person_id
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , enrollment_start_date
    , enrollment_end_date
    , tuva_last_run
    {{ select_extension_columns(ref('normalized__eligibility'), alias='elig') }}
    , data_source
  from {{ ref('normalized__eligibility') }} as elig
)

, eligibility_with_effective_end_date as (
  select
    *
    , case
        when enrollment_end_date is null
          or enrollment_end_date > cast(tuva_last_run as date)
          then cast(tuva_last_run as date)
        else enrollment_end_date
      end as effective_enrollment_end_date
  from stg_eligibility
)

, month_start_and_end_dates as (
  select
      year_month
    , first_day_of_month as month_start_date
    , last_day_of_month as month_end_date
  from {{ ref('member_month__month_spine') }}
)

, joined as (
select distinct
  a.person_id
  , a.member_id
  , b.year_month
  , a.payer
  , a.{{ quote_column('plan') }}
  , a.tuva_last_run
  {{ select_extension_columns(ref('normalized__eligibility'), alias='a') }}
  , a.data_source
from eligibility_with_effective_end_date as a
inner join month_start_and_end_dates as b
  on a.enrollment_start_date <= b.month_end_date
  and a.effective_enrollment_end_date >= b.month_start_date
  and a.enrollment_start_date <= a.effective_enrollment_end_date
)

select
  cast(
    {{ dbt_utils.generate_surrogate_key([
        'person_id',
        'member_id',
        'year_month',
        'payer',
        quote_column('plan'),
        'data_source'
    ]) }}
    as {{ dbt.type_string() }}
  ) as member_month_id
, *
from joined
