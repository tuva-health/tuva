{{ config(
     enabled = (var('provider_attribution_enabled', False) and var('claims_enabled', False))
   )
}}

with mm as (
  select
      person_id
    , data_source
        {% if target.type == 'athena' %}
        , substr(year_month, 1,4) as performance_year
        {% else %}
        , left(year_month, 4) as performance_year
        {% endif %}
  from {{ ref('enrollment__member_month') }}
  group by person_id
        , data_source
        {% if target.type == 'athena' %}
        , substr(year_month, 1,4)
        {% else %}
        , left(year_month, 4)
        {% endif %}
)
select
    cast(person_id as {{ dbt.type_string() }}) as person_id
  , cast(data_source as {{ dbt.type_string() }}) as data_source
  , cast(performance_year as {{ dbt.type_int() }}) as performance_year
from mm
