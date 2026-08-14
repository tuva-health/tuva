{{ config(
     enabled = (var('provider_attribution_enabled', False) and var('claims_enabled', False))
   )
}}

select
    mm.person_id
  , mm.data_source
  , mm.year_month
  , mm.tuva_attributed_provider
  , mm.tuva_attributed_provider_bucket
  , mm.tuva_attributed_provider_specialty
from {{ ref('core__member_month') }} as mm
left outer join {{ ref('provider_attribution__assigned_beneficiary_yearly') }} as attr
  on mm.person_id = attr.person_id
  and mm.data_source = attr.data_source
  {% if target.type == 'athena' %}
  and cast(substr(mm.year_month, 1, 4) as {{ dbt.type_int() }}) = attr.performance_year
  {% else %}
  and cast(left(mm.year_month, 4) as {{ dbt.type_int() }}) = attr.performance_year
  {% endif %}
where coalesce(
      mm.tuva_attributed_provider
    , mm.tuva_attributed_provider_bucket
    , mm.tuva_attributed_provider_specialty
  ) is not null
  and (
      attr.person_id is null
      or coalesce(mm.tuva_attributed_provider, '__null__') != coalesce(
        case
          when attr.provider_bucket = 'no_eligible_history' then null
          else attr.provider_id
        end
      , '__null__')
      or coalesce(mm.tuva_attributed_provider_bucket, '__null__') != coalesce(attr.provider_bucket, '__null__')
      or coalesce(mm.tuva_attributed_provider_specialty, '__null__') != coalesce(attr.prov_specialty, '__null__')
  )
