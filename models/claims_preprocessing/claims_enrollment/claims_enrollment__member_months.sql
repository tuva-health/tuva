{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


with stg_eligibility as (
  select
    person_id
    , payer
    , {{ quote_column('plan') }}
    , data_source
    , min(enrollment_start_date) as min_enrollment_start_date
    , max(enrollment_end_date) as max_enrollment_end_date
  from {{ ref('normalized_input__eligibility') }} as elig
  group by 
    person_id
    , payer
    , {{ quote_column('plan') }}
    , data_source
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
select 
  -- Generate a unique key for each member month
  dense_rank() over (
    order by
      a.person_id
    , b.year_month
    , a.payer
    , a.{{ quote_column('plan') }}
    , a.data_source
    ) as member_month_key
  , a.person_id
  -- As a temporary fix, we are nulling out member_id to get to the grain we want. 
  -- In a future release we will remove this field.
  , cast(null as {{ dbt.type_string() }}) as member_id 
  , b.year_month
  , a.payer
  , a.{{ quote_column('plan') }}
  , a.data_source
from stg_eligibility as a
inner join month_start_and_end_dates as b
  on a.min_enrollment_start_date <= b.month_end_date
  and a.max_enrollment_end_date >= b.month_start_date