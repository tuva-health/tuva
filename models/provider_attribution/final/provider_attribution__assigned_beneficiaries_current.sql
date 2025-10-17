{{ config(
     enabled = var('provider_attribution_enabled', var('tuva_marts_enabled', True)) | as_bool
   )
}}

with claim_bounds as (
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

, steps as (
  select * from {{ ref('provider_attribution__int_current_steps') }}
)

, ranked as (
  select 
      s.person_id
    , s.provider_id
    , s.provider_bucket
    , max(s.prov_specialty) over (partition by s.person_id, s.provider_id) as prov_specialty
    , s.step as assigned_step
    , s.allowed_amount
    , s.visits
    , rank() over (partition by s.person_id order by s.allowed_amount desc, s.visits desc, s.provider_id) as provider_rank
  from steps s
)

, months as (
  -- Build the last 12 calendar months (YYYYMM) ending at as_of_date
  select distinct 
      {{ concat_custom(["c.year", dbt.right(concat_custom(["'0'", "c.month"]), 2)]) }} as year_month
  from {{ ref('reference_data__calendar') }} c
  cross join params p
  where c.full_date >= cast({{ dbt.dateadd(datepart='month', interval=-11, from_date_or_timestamp='p.as_of_date') }} as date)
    and c.full_date <= p.as_of_date
)

, eligible as (
  select distinct mm.person_id
  from {{ ref('core__member_months') }} mm
  inner join months m
    on mm.year_month = m.year_month
)

, assigned as (
  select 
      r.person_id
    , p.as_of_date
    , r.provider_id
    , r.provider_bucket
    , r.prov_specialty
    , r.assigned_step
    , r.allowed_amount
    , r.visits
    , cast({{ dbt.dateadd(datepart='month', interval=-11, from_date_or_timestamp='p.as_of_date') }} as date) as lookback_start_date
    , p.as_of_date as lookback_end_date
    , {{ concat_custom(["'current|'", "replace(cast(p.as_of_date as " ~ dbt.type_string() ~ "),'-','')", "'|'", "r.person_id"]) }} as attribution_key
  from ranked r
  cross join params p
  where r.provider_rank = 1
)

, missing as (
  select 
      e.person_id
  from eligible e
  left join assigned a
    on e.person_id = a.person_id
  where a.person_id is null
)

, fallback as (
  select 
      m.person_id
    , p.as_of_date
    , '9999999999' as provider_id
    , 'no_history' as provider_bucket
    , 'No assignable claims history' as prov_specialty
    , 0 as assigned_step
    , cast(0 as {{ dbt.type_numeric() }}) as allowed_amount
    , 0 as visits
    , cast({{ dbt.dateadd(datepart='month', interval=-11, from_date_or_timestamp='p.as_of_date') }} as date) as lookback_start_date
    , p.as_of_date as lookback_end_date
    , {{ concat_custom(["'current|'", "replace(cast(p.as_of_date as " ~ dbt.type_string() ~ "),'-','')", "'|'", "m.person_id"]) }} as attribution_key
  from missing m
  cross join params p
)

select 
    person_id
  , as_of_date
  , provider_id
  , provider_bucket
  , prov_specialty
  , assigned_step
  , allowed_amount
  , visits
  , lookback_start_date
  , lookback_end_date
  , attribution_key
from assigned

union all

select 
    person_id
  , as_of_date
  , provider_id
  , provider_bucket
  , prov_specialty
  , assigned_step
  , allowed_amount
  , visits
  , lookback_start_date
  , lookback_end_date
  , attribution_key
from fallback
