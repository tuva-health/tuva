{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
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
  from {{ ref('member_month__member_month') }}
)

, external_attribution as (
  select
      person_id
    , member_id
    , year_month
    , payer
    , {{ quote_column('plan') }}
    , data_source
    , payer_attributed_provider
    , payer_attributed_provider_practice
    , payer_attributed_provider_organization
    , payer_attributed_provider_lob
    , custom_attributed_provider
    , custom_attributed_provider_practice
    , custom_attributed_provider_organization
    , custom_attributed_provider_lob
  from {{ ref('normalized__provider_attribution') }}
)

{% if the_tuva_project.tuva_boolean_var('provider_attribution_enabled', false) -%}

, tuva_attribution as (
  select
      person_id
    , member_id
    , year_month
    , payer
    , {{ quote_column('plan') }}
    , data_source
    , tuva_attributed_provider
    , tuva_attributed_provider_bucket
    , tuva_attributed_provider_specialty
    , tuva_attribution_assigned_step
    , tuva_attribution_step_description
    , tuva_attribution_allowed_amount
    , tuva_attribution_visits
    , tuva_attribution_lookback_start_date
    , tuva_attribution_lookback_end_date
    , tuva_attribution_key
  from {{ ref('provider_attribution__tuva_member_month_attribution') }}
)

{%- endif %}

select
    mm.person_id
  , mm.member_id
  , mm.year_month
  , mm.payer
  , mm.{{ quote_column('plan') }}
  , external_attribution.payer_attributed_provider
  , external_attribution.payer_attributed_provider_practice
  , external_attribution.payer_attributed_provider_organization
  , external_attribution.payer_attributed_provider_lob
  , external_attribution.custom_attributed_provider
  , external_attribution.custom_attributed_provider_practice
  , external_attribution.custom_attributed_provider_organization
  , external_attribution.custom_attributed_provider_lob
  {% if the_tuva_project.tuva_boolean_var('provider_attribution_enabled', false) -%}
  , tuva_attribution.tuva_attributed_provider
  , tuva_attribution.tuva_attributed_provider_bucket
  , tuva_attribution.tuva_attributed_provider_specialty
  , tuva_attribution.tuva_attribution_assigned_step
  , tuva_attribution.tuva_attribution_step_description
  , tuva_attribution.tuva_attribution_allowed_amount
  , tuva_attribution.tuva_attribution_visits
  , tuva_attribution.tuva_attribution_lookback_start_date
  , tuva_attribution.tuva_attribution_lookback_end_date
  , tuva_attribution.tuva_attribution_key
  {% else -%}
  , cast(null as {{ dbt.type_string() }}) as tuva_attributed_provider
  , cast(null as {{ dbt.type_string() }}) as tuva_attributed_provider_bucket
  , cast(null as {{ dbt.type_string() }}) as tuva_attributed_provider_specialty
  , cast(null as {{ dbt.type_int() }}) as tuva_attribution_assigned_step
  , cast(null as {{ dbt.type_string() }}) as tuva_attribution_step_description
  , cast(null as {{ dbt.type_numeric() }}) as tuva_attribution_allowed_amount
  , cast(null as {{ dbt.type_int() }}) as tuva_attribution_visits
  , cast(null as date) as tuva_attribution_lookback_start_date
  , cast(null as date) as tuva_attribution_lookback_end_date
  , cast(null as {{ dbt.type_string() }}) as tuva_attribution_key
  {%- endif %}
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
  , mm.data_source
from member_months as mm
left outer join external_attribution
  on mm.person_id = external_attribution.person_id
  and mm.member_id = external_attribution.member_id
  and mm.year_month = external_attribution.year_month
  and mm.payer = external_attribution.payer
  and mm.{{ quote_column('plan') }} = external_attribution.{{ quote_column('plan') }}
  and mm.data_source = external_attribution.data_source
{% if the_tuva_project.tuva_boolean_var('provider_attribution_enabled', false) -%}
left outer join tuva_attribution
  on mm.person_id = tuva_attribution.person_id
  and mm.member_id = tuva_attribution.member_id
  and mm.year_month = tuva_attribution.year_month
  and mm.payer = tuva_attribution.payer
  and mm.{{ quote_column('plan') }} = tuva_attribution.{{ quote_column('plan') }}
  and mm.data_source = tuva_attribution.data_source
{%- endif %}
