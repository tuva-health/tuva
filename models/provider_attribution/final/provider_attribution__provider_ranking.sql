{{ config(
     enabled = var('provider_attribution_enabled', var('tuva_marts_enabled', True)) | as_bool
   )
}}

with yearly as (
  select 
      s.person_id
    , cast(s.performance_year as {{ dbt.type_int() }}) as performance_year
    , null as as_of_date
    , s.provider_id
    , s.provider_bucket
    , s.prov_specialty
    , s.step
    , s.allowed_amount
    , s.visits
    , 'yearly' as scope
    , case 
        when s.step = 3
          then cast(concat(cast(s.performance_year - 1 as {{ dbt.type_string() }}),'01','01') as date)
        else cast(concat(cast(s.performance_year as {{ dbt.type_string() }}),'01','01') as date)
      end as lookback_start_date
    , cast(concat(cast(s.performance_year as {{ dbt.type_string() }}),'12','31') as date) as lookback_end_date
    , {{ concat_custom(["'yearly|'", "cast(s.performance_year as " ~ dbt.type_string() ~ ")", "'|'", "s.person_id"]) }} as attribution_key
    , rank() over (partition by s.person_id, s.performance_year order by s.allowed_amount desc, s.visits desc, s.provider_id) as ranking
  from {{ ref('provider_attribution__int_yearly_steps') }} s
)

, claim_bounds as (
  select max(claim_end_date) as max_claim_end_date
  from {{ ref('provider_attribution__int_primary_care_claims') }}
)

, params as (
  select case
           when max_claim_end_date is not null
             and max_claim_end_date <= cast({{ dbt.current_timestamp() }} as date)
             then max_claim_end_date
           else cast({{ dbt.current_timestamp() }} as date)
         end as as_of_date
  from claim_bounds
)

, current_scope as (
  select 
      s.person_id
    , null as performance_year
    , p.as_of_date
    , s.provider_id
    , s.provider_bucket
    , s.prov_specialty
    , s.step
    , s.allowed_amount as allowed_amount
    , s.visits
    , 'current' as scope
    , cast({{ dbt.dateadd(datepart='month', interval=-11, from_date_or_timestamp='p.as_of_date') }} as date) as lookback_start_date
    , p.as_of_date as lookback_end_date
    , {{ concat_custom(["'current|'", "replace(cast(p.as_of_date as " ~ dbt.type_string() ~ "),'-','')", "'|'", "s.person_id"]) }} as attribution_key
    , rank() over (partition by s.person_id order by s.allowed_amount desc, s.visits desc, s.provider_id) as ranking
  from {{ ref('provider_attribution__int_current_steps') }} s
  cross join params p
)

select 
    person_id
  , performance_year
  , as_of_date
  , provider_id
  , provider_bucket
  , prov_specialty
  , step
  , allowed_amount
  , visits
  , scope
  , lookback_start_date
  , lookback_end_date
  , ranking
  , attribution_key
from yearly

union all

select 
    person_id
  , performance_year
  , as_of_date
  , provider_id
  , provider_bucket
  , prov_specialty
  , step
  , allowed_amount
  , visits
  , scope
  , lookback_start_date
  , lookback_end_date
  , ranking
  , attribution_key
from current_scope
