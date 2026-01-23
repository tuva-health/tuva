{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


with stg_eligibility as (
  select
    person_id
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , data_source
    , enrollment_start_date
    , enrollment_end_date
    , tuva_last_run
  from {{ ref('normalized_input__eligibility') }} as elig
)

, month_start_and_end_dates as (
  select
    {{ concat_custom(["year",
                  dbt.right(concat_custom(["'0'", "month"]), 2)]) }} as year_month
    , min(full_date) as month_start_date
    , max(full_date) as month_end_date
  from {{ ref('reference_data__calendar') }}
  group by year, month, year_month
)

, joined as (
select distinct
  a.person_id
  , a.member_id
  , b.year_month
  , a.payer
  , a.{{ quote_column('plan') }}
  , a.data_source
  , a.tuva_last_run
from stg_eligibility as a
inner join month_start_and_end_dates as b
  on a.enrollment_start_date <= b.month_end_date
  and a.enrollment_end_date >= b.month_start_date
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
  ) as member_month_key
, *
from joined
