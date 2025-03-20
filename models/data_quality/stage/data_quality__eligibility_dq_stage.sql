{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with month_start_and_end_dates as (
  select
      year_month_int as year_month
    , min(full_date) as month_start_date
    , max(full_date) as month_end_date
  from {{ ref('reference_data__calendar')}}
  group by year_month_int
)

select distinct
    a.person_id
  , b.year_month
  , b.month_start_date
  , b.month_end_date
  , a.payer
  , a.{{ quote_column('plan') }}
  , data_source
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('input_layer__eligibility') }} a
inner join month_start_and_end_dates b
  on a.enrollment_start_date <= b.month_end_date
  and a.enrollment_end_date >= b.month_start_date
