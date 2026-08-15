{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

{% if (var('provider_attribution_enabled', False) | string | lower) == 'true' -%}

select
      cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(year_month as {{ dbt.type_string() }}) as year_month
    , cast(payer as {{ dbt.type_string() }}) as payer
    , cast({{ quote_column('plan') }} as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
    , cast(payer_attributed_provider as {{ dbt.type_string() }}) as payer_attributed_provider
    , cast(payer_attributed_provider_practice as {{ dbt.type_string() }}) as payer_attributed_provider_practice
    , cast(payer_attributed_provider_organization as {{ dbt.type_string() }}) as payer_attributed_provider_organization
    , cast(payer_attributed_provider_lob as {{ dbt.type_string() }}) as payer_attributed_provider_lob
    , cast(custom_attributed_provider as {{ dbt.type_string() }}) as custom_attributed_provider
    , cast(custom_attributed_provider_practice as {{ dbt.type_string() }}) as custom_attributed_provider_practice
    , cast(custom_attributed_provider_organization as {{ dbt.type_string() }}) as custom_attributed_provider_organization
    , cast(custom_attributed_provider_lob as {{ dbt.type_string() }}) as custom_attributed_provider_lob
    , cast(file_name as {{ dbt.type_string() }}) as file_name
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    , cast(data_source as {{ dbt.type_string() }}) as data_source
from {{ ref('stg_input_layer__provider_attribution') }}

{% else -%}

{% if target.type == 'fabric' %}
select top 0
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
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
    , cast(null as {{ dbt.type_string() }} ) as data_source
{% else %}
select
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
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
    , cast(null as {{ dbt.type_string() }} ) as data_source
limit 0
{%- endif %}

{%- endif %}
