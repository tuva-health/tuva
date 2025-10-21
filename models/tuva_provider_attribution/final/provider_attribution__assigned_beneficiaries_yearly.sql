{{ config(
     enabled = var('tuva_provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with steps as (
  select * from {{ ref('provider_attribution__int_yearly_steps') }}
)

, ranked as (
  select
      person_id
    , performance_year
    , provider_id
    , provider_bucket
    , max(prov_specialty) over (partition by person_id, performance_year, provider_id) as prov_specialty
    , step as assigned_step
    , allowed_amount
    , visits
    , rank() over (partition by person_id, performance_year
order by allowed_amount desc, visits desc, provider_id) as provider_rank
  from steps
)

, eligible as (
  select
      person_id
    , performance_year
  from {{ ref('provider_attribution__int_person_years') }}
)

, calendar_months as (
  select distinct
      year_month_int
    , first_day_of_month
    , last_day_of_month
  from {{ ref('provider_attribution__stg_reference_data__calendar') }}
)

, assigned as (
  select
      person_id
    , performance_year
    , provider_id
    , provider_bucket
    , prov_specialty
    , assigned_step
    , allowed_amount
    , visits
    , case
        when assigned_step = 3
          then coalesce(start_prev.first_day_of_month, start_curr.first_day_of_month)
        else start_curr.first_day_of_month
      end as lookback_start_date
    , end_curr.last_day_of_month as lookback_end_date
    , {{ concat_custom(["'yearly|'", "cast(performance_year as " ~ dbt.type_string() ~ ")", "'|'", "person_id"]) }} as attribution_key
  from ranked
  left outer join calendar_months as start_curr
    on start_curr.year_month_int = (performance_year * 100) + 1
  left outer join calendar_months as start_prev
    on start_prev.year_month_int = ((performance_year - 1) * 100) + 1
  left outer join calendar_months as end_curr
    on end_curr.year_month_int = (performance_year * 100) + 12
  where provider_rank = 1
)

, missing as (
  select
      e.person_id
    , e.performance_year
  from eligible as e
  left outer join assigned as a
    on e.person_id = a.person_id
   and e.performance_year = a.performance_year
  where a.person_id is null
)

, fallback as (
  select
      person_id
    , performance_year
    , '9999999999' as provider_id
    , 'no_eligible_history' as provider_bucket
    , 'No assignable claims history' as prov_specialty
    , 0 as assigned_step
    , cast(0 as {{ dbt.type_numeric() }}) as allowed_amount
    , 0 as visits
    -- No prior utilization exists, so anchor placeholders to the current year's window.
    , start_curr.first_day_of_month as lookback_start_date
    , end_curr.last_day_of_month as lookback_end_date
    , {{ concat_custom(["'yearly|'", "cast(performance_year as " ~ dbt.type_string() ~ ")", "'|'", "person_id"]) }} as attribution_key
  from missing
  left outer join calendar_months as start_curr
    on start_curr.year_month_int = (performance_year * 100) + 1
  left outer join calendar_months as end_curr
    on end_curr.year_month_int = (performance_year * 100) + 12
)

select
    person_id
  , performance_year
  , provider_id
  , provider_bucket
  , prov_specialty
  , assigned_step
  , allowed_amount
  , visits
  , lookback_start_date
  , lookback_end_date
  , attribution_key
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from assigned

union all

select
    person_id
  , performance_year
  , provider_id
  , provider_bucket
  , prov_specialty
  , assigned_step
  , allowed_amount
  , visits
  , lookback_start_date
  , lookback_end_date
  , attribution_key
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from fallback
