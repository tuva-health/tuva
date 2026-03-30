{{ config(
     enabled = (
         var('provider_attribution_enabled', False) == True and
         var('claims_enabled', var('tuva_marts_enabled', False))
     ) | as_bool
   )
}}

{# Extension columns not supported for provider_attribution #}
{%- set tuva_extensions -%}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
{%- endset -%}

{%- set provider_attribution_relation = ref('provider_attribution_seed') -%}

{%- if execute -%}
  {%- set provider_attribution_columns = adapter.get_columns_in_relation(provider_attribution_relation) -%}
  {%- set provider_attribution_column_names = provider_attribution_columns | map(attribute='name') | map('lower') | list -%}
{%- else -%}
  {%- set provider_attribution_column_names = [] -%}
{%- endif -%}

{%- set member_id_expr -%}
  {%- if 'member_id' in provider_attribution_column_names -%}
    member_id
  {%- else -%}
    person_id
  {%- endif -%}
{%- endset -%}

select
      person_id
    , {{ member_id_expr }} as member_id
    , patient_id
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
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ provider_attribution_relation }}
