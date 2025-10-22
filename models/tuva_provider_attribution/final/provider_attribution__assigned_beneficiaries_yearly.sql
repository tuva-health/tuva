{{ config(
     enabled = var('tuva_provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with eligible as (
  select
      person_id
    , performance_year
  from {{ ref('provider_attribution__int_person_years') }}
)

, assigned as (
  select
      pr.person_id
    , pr.performance_year
    , pr.provider_id
    , pr.provider_bucket
    , pr.prov_specialty
    , pr.step as assigned_step
    , pr.step_description
    , pr.allowed_amount
    , pr.visits
    , pr.lookback_start_date
    , pr.lookback_end_date
    , pr.attribution_key
  from {{ ref('provider_attribution__provider_ranking') }} as pr
  where pr.scope = 'yearly'
    and pr.ranking = 1
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
    , 'No assignable history' as step_description
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
  , step_description
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
  , step_description
  , allowed_amount
  , visits
  , lookback_start_date
  , lookback_end_date
  , attribution_key
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from fallback
