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
, member_months_deduped as (
  select
      a.person_id
    , max(a.member_id) as member_id
    , b.year_month
    , a.payer
    , a.{{ quote_column('plan') }}
    , a.data_source
    , max(a.tuva_last_run) as tuva_last_run
  from stg_eligibility as a
  inner join month_start_and_end_dates as b
    on a.enrollment_start_date <= b.month_end_date
    and a.enrollment_end_date >= b.month_start_date
  group by
      a.person_id
    , b.year_month
    , a.payer
    , a.{{ quote_column('plan') }}
    , a.data_source
)

select
  dense_rank() over (
    order by
      person_id
    , year_month
    , payer
    , {{ quote_column('plan') }}
    , data_source
    ) as member_month_key
  , person_id
  , member_id
  , year_month
  , payer
  , {{ quote_column('plan') }}
  , data_source
  , tuva_last_run
from member_months_deduped
