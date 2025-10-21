{{ config(
     enabled = var('tuva_provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with mm as (
  select
      person_id
    , left(year_month, 4) as performance_year
  from {{ ref('provider_attribution__stg_core__member_months') }}
  group by person_id, left(year_month, 4)
)

select
    cast(person_id as {{ dbt.type_string() }}) as person_id
  , cast(performance_year as {{ dbt.type_int() }}) as performance_year
from mm
