{{ config(
     enabled = ((var('provider_attribution_enabled', False) | string | lower) == 'true')
           and ((var('claims_enabled', False) | string | lower) == 'true')
   )
}}

with yearly_variation as (
  select
      person_id
    , data_source
    {% if target.type == 'athena' %}
    , substr(year_month, 1, 4) as performance_year
    {% else %}
    , left(year_month, 4) as performance_year
    {% endif %}
    , count(distinct coalesce(tuva_attributed_provider, '__null__')) as provider_values
    , count(distinct coalesce(tuva_attributed_provider_bucket, '__null__')) as provider_bucket_values
    , count(distinct coalesce(tuva_attributed_provider_specialty, '__null__')) as provider_specialty_values
  from {{ ref('core__member_month') }}
  group by
      person_id
    , data_source
    {% if target.type == 'athena' %}
    , substr(year_month, 1, 4)
    {% else %}
    , left(year_month, 4)
    {% endif %}
)

select
    person_id
  , data_source
  , performance_year
  , provider_values
  , provider_bucket_values
  , provider_specialty_values
from yearly_variation
where provider_values > 1
   or provider_bucket_values > 1
   or provider_specialty_values > 1
