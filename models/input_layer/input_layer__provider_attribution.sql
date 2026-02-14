{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

{% if var('provider_attribution_enabled',False) == true -%}

select *
from {{ ref('provider_attribution') }}


{% elif var('provider_attribution_enabled',False) ==  false -%}

{% if target.type == 'fabric' %}
select top 0
      cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as member_id
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
    , cast(null as {{ dbt.type_string() }} ) as tuva_last_run
{% else %}
select
      cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as member_id
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
    , cast(null as {{ dbt.type_string() }} ) as tuva_last_run
limit 0
{%- endif %}

{%- endif %}
