{{ config(
     enabled = var('provider_attribution_enabled', var('tuva_marts_enabled', True)) | as_bool
   )
}}

with params as (
  select cast(substring('{{ var('tuva_last_run') }}',1,10) as date) as as_of_date
)

, steps as (
  select * from {{ ref('provider_attribution__int_current_steps') }}
)

, ranked as (
  select 
      s.person_id
    , s.provider_id
    , s.bucket as assigned_bucket
    , max(s.prov_specialty) over (partition by s.person_id, s.provider_id) as prov_specialty
    , s.step as assigned_step
    , s.allowed_charges
    , s.visits
    , rank() over (partition by s.person_id order by s.allowed_charges desc, s.visits desc, s.provider_id) as provider_rank
  from steps s
)

select 
    r.person_id
  , p.as_of_date
  , r.provider_id
  , r.assigned_bucket
  , r.prov_specialty
  , r.assigned_step
  , r.allowed_charges
  , r.visits
  , cast({{ dbt.dateadd(datepart='month', interval=-11, from_date_or_timestamp='p.as_of_date') }} as date) as window_start
  , p.as_of_date as window_end
  , {{ concat_custom(["'current|'", "replace(cast(p.as_of_date as " ~ dbt.type_string() ~ "),'-','')", "'|'", "r.person_id"]) }} as attribution_key
from ranked r
cross join params p
where r.provider_rank = 1
