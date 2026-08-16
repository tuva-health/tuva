{{ config(
     enabled = ((var('provider_attribution_enabled', False) | string | lower) == 'true')
           and ((var('claims_enabled', False) | string | lower) == 'true')
   )
}}

with mm as (
  select
      person_id
    , data_source
        , {{ left_chars('year_month', 4) }} as performance_year
  from {{ ref('enrollment__member_month') }}
  group by person_id
        , data_source
        , {{ left_chars('year_month', 4) }}
)
select
    cast(person_id as {{ dbt.type_string() }}) as person_id
  , cast(data_source as {{ dbt.type_string() }}) as data_source
  , cast(performance_year as {{ dbt.type_int() }}) as performance_year
from mm
