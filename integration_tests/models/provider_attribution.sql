{{ config(
     enabled = (
         var('provider_attribution_enabled', False) == True and
         var('claims_enabled', False)
     ) | as_bool
   )
}}

{%- set tuva_columns -%}
      person_id
    , member_id
    , year_month
    , payer
    , {{ the_tuva_project.quote_column('plan') }}
    , payer_attributed_provider
    , payer_attributed_provider_practice
    , payer_attributed_provider_organization
    , payer_attributed_provider_lob
    , custom_attributed_provider
    , custom_attributed_provider_practice
    , custom_attributed_provider_organization
    , custom_attributed_provider_lob
{%- endset -%}

{# Extension columns are not supported for provider_attribution #}
{%- set tuva_extensions -%}
{%- endset -%}

{%- set tuva_metadata -%}
    , file_name
    , ingest_datetime
    , data_source
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
{% if the_tuva_project.tuva_synthetic_data_enabled() %}
-- depends_on: {{ ref('the_tuva_project', 'synthetic_data__provider_attribution') }}
{% endif %}
from {{ source('source_input', 'provider_attribution') }}
