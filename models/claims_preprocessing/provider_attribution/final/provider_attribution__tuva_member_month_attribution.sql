{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('provider_attribution_enabled', false) and the_tuva_project.tuva_boolean_var('claims_enabled', false))
   )
}}

with member_months as (
  select
      person_id
    , member_id
    , year_month
    , payer
    , {{ quote_column('plan') }}
    , data_source
    {% if target.type == 'athena' %}
    , cast(substr(year_month, 1, 4) as {{ dbt.type_int() }}) as performance_year
    {% else %}
    , cast(left(year_month, 4) as {{ dbt.type_int() }}) as performance_year
    {% endif %}
  from {{ ref('member_month__member_month') }}
)

select
    mm.person_id
  , mm.member_id
  , mm.year_month
  , mm.payer
  , mm.{{ quote_column('plan') }}
  , mm.data_source
  , case
      when attr.provider_bucket = 'no_eligible_history' then null
      else attr.provider_id
    end as tuva_attributed_provider
  , attr.provider_bucket as tuva_attributed_provider_bucket
  , attr.prov_specialty as tuva_attributed_provider_specialty
  , attr.assigned_step as tuva_attribution_assigned_step
  , attr.step_description as tuva_attribution_step_description
  , attr.allowed_amount as tuva_attribution_allowed_amount
  , attr.visits as tuva_attribution_visits
  , attr.lookback_start_date as tuva_attribution_lookback_start_date
  , attr.lookback_end_date as tuva_attribution_lookback_end_date
  , attr.attribution_key as tuva_attribution_key
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from member_months as mm
left outer join {{ ref('provider_attribution__assigned_beneficiaries_yearly') }} as attr
  on mm.person_id = attr.person_id
  and mm.data_source = attr.data_source
  and mm.performance_year = attr.performance_year
