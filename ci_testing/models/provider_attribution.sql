{{ config(
     enabled = var('provider_attribution_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

{# logic to use the seed data or an empty table -#}

{% if var('test_data_override') == true -%}

select * from {{ ref('provider_attribution_seed') }}

{%- else -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
      cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as year_month
    , cast(null as {{ dbt.type_string() }} ) as payer
    , cast(null as {{ dbt.type_string() }} ) as {{ quote_column('plan') }}
    , cast(null as {{ dbt.type_string() }} ) as data_source
    , cast(null as {{ dbt.type_string() }} ) as payer_attributed_provider
    , cast(null as {{ dbt.type_string() }} ) as payer_attributed_provider_practice
    , cast(null as {{ dbt.type_string() }} ) as payer_attributed_provider_organization
    , cast(null as {{ dbt.type_string() }} ) as payer_attributed_provider_lob
    , cast(null as {{ dbt.type_string() }} ) as custom_attributed_provider
    , cast(null as {{ dbt.type_string() }} ) as custom_attributed_provider_practice
    , cast(null as {{ dbt.type_string() }} ) as custom_attributed_provider_organization
    , cast(null as {{ dbt.type_string() }} ) as custom_attributed_provider_lob
    {% if target.type == 'fabric' %} {% else %} limit 0 {% endif %}

{%- endif %}
