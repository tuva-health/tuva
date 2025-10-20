{{ config(
     enabled = var('provider_attribution_enabled', var('tuva_marts_enabled', True)) | as_bool
   )
}}

with calendar_months as (
  select distinct
      year_month_int
    , first_day_of_month
    , last_day_of_month
  from {{ ref('provider_attribution__stg_reference_data__calendar') }}
)

, yearly as (
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
          then coalesce(start_prev.first_day_of_month, start_curr.first_day_of_month)
        else start_curr.first_day_of_month
      end as lookback_start_date
    , end_curr.last_day_of_month as lookback_end_date
    , {{ concat_custom(["'yearly|'", "cast(s.performance_year as " ~ dbt.type_string() ~ ")", "'|'", "s.person_id"]) }} as attribution_key
    , rank() over (partition by s.person_id, s.performance_year order by s.allowed_amount desc, s.visits desc, s.provider_id) as ranking
  from {{ ref('provider_attribution__int_yearly_steps') }} s
  left join calendar_months start_curr
    on start_curr.year_month_int = (s.performance_year * 100) + 1
  left join calendar_months start_prev
    on start_prev.year_month_int = ((s.performance_year - 1) * 100) + 1
  left join calendar_months end_curr
    on end_curr.year_month_int = (s.performance_year * 100) + 12
)

, claim_bounds as (
  select max(claim_end_date) as max_claim_end_date
  from {{ ref('provider_attribution__int_primary_care_claims') }}
)

{% set override_as_of_date = var('provider_attribution_as_of_date', none) %}

, params as (
  select
    {% if override_as_of_date %}
      cast('{{ override_as_of_date }}' as date) as as_of_date
    {% else %}
      case
        when max_claim_end_date is not null
          and max_claim_end_date <= cast({{ dbt.current_timestamp() }} as date)
          then max_claim_end_date
        else cast({{ dbt.current_timestamp() }} as date)
      end as as_of_date
    {% endif %}
  from claim_bounds
)

, months as (
  select distinct 
      c.year_month_int
    , c.first_day_of_month
  from {{ ref('provider_attribution__stg_reference_data__calendar') }} c
  cross join params p
  where c.full_date >= cast({{ dbt.dateadd(datepart='month', interval=-11, from_date_or_timestamp='p.as_of_date') }} as date)
    and c.full_date <= p.as_of_date
)

, lookback_bounds as (
  select 
      min(first_day_of_month) as lookback_start_date
  from months
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
    , lb.lookback_start_date as lookback_start_date
    , p.as_of_date as lookback_end_date
    , {{ concat_custom(["'current|'", "replace(cast(p.as_of_date as " ~ dbt.type_string() ~ "),'-','')", "'|'", "s.person_id"]) }} as attribution_key
    , rank() over (partition by s.person_id order by s.allowed_amount desc, s.visits desc, s.provider_id) as ranking
  from {{ ref('provider_attribution__int_current_steps') }} s
  cross join params p
  cross join lookback_bounds lb
)

, yearly_placeholder as (
  select 
      person_id
    , cast(performance_year as {{ dbt.type_int() }}) as performance_year
    , null as as_of_date
    , provider_id
    , provider_bucket
    , prov_specialty
    , assigned_step as step
    , allowed_amount
    , visits
    , 'yearly' as scope
    , lookback_start_date
    , lookback_end_date
    , 1 as ranking
    , attribution_key
  from {{ ref('provider_attribution__assigned_beneficiaries_yearly') }}
  where provider_id = '9999999999'
)

, current_placeholder as (
  select 
      person_id
    , null as performance_year
    , as_of_date
    , provider_id
    , provider_bucket
    , prov_specialty
    , assigned_step as step
    , allowed_amount
    , visits
    , 'current' as scope
    , lookback_start_date
    , lookback_end_date
    , 1 as ranking
    , attribution_key
  from {{ ref('provider_attribution__assigned_beneficiaries_current') }}
  where provider_id = '9999999999'
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
from yearly_placeholder

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
from current_placeholder
