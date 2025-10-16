{{ config(
     enabled = var('provider_attribution_enabled', var('tuva_marts_enabled', True)) | as_bool
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
    , bucket as assigned_bucket
    , max(prov_specialty) over (partition by person_id, performance_year, provider_id) as prov_specialty
    , step as assigned_step
    , allowed_charges
    , visits
    , rank() over (partition by person_id, performance_year order by allowed_charges desc, visits desc, provider_id) as provider_rank
  from steps
)

select 
    person_id
  , performance_year
  , provider_id
  , assigned_bucket
  , prov_specialty
  , assigned_step
  , allowed_charges
  , visits
  , cast(concat(cast(performance_year as {{ dbt.type_string() }}),'01','01') as date) as window_start
  , cast(concat(cast(performance_year as {{ dbt.type_string() }}),'12','31') as date) as window_end
  , {{ concat_custom(["'yearly|'", "cast(performance_year as " ~ dbt.type_string() ~ ")", "'|'", "person_id"]) }} as attribution_key
from ranked
where provider_rank = 1
