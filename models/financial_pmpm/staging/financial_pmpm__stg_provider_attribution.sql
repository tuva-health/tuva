{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

{% if var('provider_attribution_enabled',False) == true -%}

select
      cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(year_month as {{ dbt.type_string() }}) as year_month
    , cast(payer as {{ dbt.type_string() }}) as payer
    , {{ quote_column('plan') }}
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(payer_attributed_provider as {{ dbt.type_string() }}) as payer_attributed_provider
    , cast(payer_attributed_provider_practice as {{ dbt.type_string() }}) as payer_attributed_provider_practice
    , cast(payer_attributed_provider_organization as {{ dbt.type_string() }}) as payer_attributed_provider_organization
    , cast(payer_attributed_provider_lob as {{ dbt.type_string() }}) as payer_attributed_provider_lob
    , cast(custom_attributed_provider as {{ dbt.type_string() }}) as custom_attributed_provider
    , cast(custom_attributed_provider_practice as {{ dbt.type_string() }}) as custom_attributed_provider_practice
    , cast(custom_attributed_provider_organization as {{ dbt.type_string() }}) as custom_attributed_provider_organization
    , cast(custom_attributed_provider_lob as {{ dbt.type_string() }}) as custom_attributed_provider_lob
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('input_layer__provider_attribution') }}

{% elif var('provider_attribution_enabled',False) ==  false -%}

{% if target.type == 'fabric' %}
select top 0
      cast(null as {{ dbt.type_string() }} ) person_id
    , cast(null as {{ dbt.type_string() }} ) as member_id
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
    , cast(null as {{ dbt.type_string() }} ) as tuva_last_run
{% else %}
select
      cast(null as {{ dbt.type_string() }} ) person_id
    , cast(null as {{ dbt.type_string() }} ) as member_id
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
    , cast(null as {{ dbt.type_string() }} ) as tuva_last_run
limit 0
{%- endif %}

{%- endif %}
