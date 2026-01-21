{{ config(
     enabled = (
         var('provider_attribution_enabled', False) == True and
         var('claims_enabled', var('tuva_marts_enabled', False))
     ) | as_bool
   )
}}

{%- set tuva_columns -%}
      person_id
    , patient_id
    , year_month
    , payer
    , plan
    , payer_attributed_provider
    , payer_attributed_provider_practice
    , payer_attributed_provider_organization
    , payer_attributed_provider_lob
    , custom_attributed_provider
    , custom_attributed_provider_practice
    , custom_attributed_provider_organization
    , custom_attributed_provider_lob
{%- endset -%}

{# Extension columns not supported for provider_attribution #}
{%- set tuva_extensions -%}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
{%- endset -%}

{% if var('use_synthetic_data') == true -%}

select * from {{ ref('provider_attribution_seed') }}

{%- else -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ source('source_input', 'provider_attribution') }}

{%- endif %}
