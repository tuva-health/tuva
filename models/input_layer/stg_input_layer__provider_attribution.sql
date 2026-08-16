{{ config(
     enabled = ((var('provider_attribution_enabled', False) | string | lower) == 'true')
           and ((var('claims_enabled', False) | string | lower) == 'true')
   )
}}

{% if (var('provider_attribution_enabled', False) | string | lower) == 'true' -%}

select *
from {{ ref('provider_attribution') }}


{% elif not ((var('provider_attribution_enabled', False) | string | lower) == 'true') -%}

{#- Fabric has no LIMIT; it spells an empty result `select top 0`. Keep one
    column list rather than two that must be edited in lockstep. -#}
{%- set fabric = target.type == 'fabric' -%}
select {% if fabric %}top 0{% endif %}
      cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as member_id
    , cast(null as {{ dbt.type_string() }} ) as year_month
    , cast(null as {{ dbt.type_string() }} ) as payer
    , cast(null as {{ dbt.type_string() }} ) as {{ quote_column('plan') }}
    , cast(null as {{ dbt.type_string() }} ) as payer_attributed_provider
    , cast(null as {{ dbt.type_string() }} ) as payer_attributed_provider_practice
    , cast(null as {{ dbt.type_string() }} ) as payer_attributed_provider_organization
    , cast(null as {{ dbt.type_string() }} ) as payer_attributed_provider_lob
    , cast(null as {{ dbt.type_string() }} ) as custom_attributed_provider
    , cast(null as {{ dbt.type_string() }} ) as custom_attributed_provider_practice
    , cast(null as {{ dbt.type_string() }} ) as custom_attributed_provider_organization
    , cast(null as {{ dbt.type_string() }} ) as custom_attributed_provider_lob
    , cast(null as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
    , cast(null as {{ dbt.type_string() }} ) as data_source
{% if not fabric %}limit 0{% endif %}

{%- endif %}
