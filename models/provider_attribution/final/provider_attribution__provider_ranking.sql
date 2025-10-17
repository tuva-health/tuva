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
    , s.bucket
    , s.prov_specialty
    , s.step
    , s.allowed_charges
    , s.visits
    , 'yearly' as scope
    , {{ concat_custom(["'yearly|'", "cast(s.performance_year as " ~ dbt.type_string() ~ ")", "'|'", "s.person_id"]) }} as attribution_key
    , rank() over (partition by s.person_id, s.performance_year order by s.allowed_charges desc, s.visits desc, s.provider_id) as ranking
  from {{ ref('provider_attribution__int_yearly_steps') }} s
)

, params as (
  select cast(substring('{{ var('tuva_last_run') }}',1,10) as date) as as_of_date
)

, current_scope as (
  select 
      s.person_id
    , null as performance_year
    , p.as_of_date
    , s.provider_id
    , s.bucket
    , s.prov_specialty
    , s.step
    , s.allowed_charges as charges
    , s.visits
    , 'current' as scope
    , {{ concat_custom(["'current|'", "replace(cast(p.as_of_date as " ~ dbt.type_string() ~ "),'-','')", "'|'", "s.person_id"]) }} as attribution_key
    , rank() over (partition by s.person_id order by s.allowed_charges desc, s.visits desc, s.provider_id) as ranking
  from {{ ref('provider_attribution__int_current_steps') }} s
  cross join params p
)

select 
    person_id
  , performance_year
  , as_of_date
  , provider_id
  , bucket
  , prov_specialty
  , step
  , allowed_charges as charges
  , visits
  , scope
  , ranking
  , attribution_key
from yearly

union all

select 
    person_id
  , performance_year
  , as_of_date
  , provider_id
  , bucket
  , prov_specialty
  , step
  , charges
  , visits
  , scope
  , ranking
  , attribution_key
from current_scope
