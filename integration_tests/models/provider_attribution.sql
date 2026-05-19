{{ config(
     enabled = (
         var('provider_attribution_enabled', False) == True and
         var('claims_enabled', False)
     ) | as_bool
   )
}}

{# Extension columns not supported for provider_attribution #}
{%- set tuva_extensions -%}
{%- endset -%}

{%- set provider_attribution_relation = ref('the_tuva_project', 'synthetic_data__provider_attribution') -%}

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

{%- set file_name_expr -%}
  {%- if 'file_name' in provider_attribution_column_names -%}
    file_name
  {%- else -%}
    cast(null as {{ dbt.type_string() }})
  {%- endif -%}
{%- endset -%}

{%- set ingest_datetime_expr -%}
  {%- if 'ingest_datetime' in provider_attribution_column_names -%}
    ingest_datetime
  {%- else -%}
    cast(null as {{ dbt.type_timestamp() }})
  {%- endif -%}
{%- endset -%}

{%- set tuva_metadata -%}
    , {{ file_name_expr }} as file_name
    , {{ ingest_datetime_expr }} as ingest_datetime
    , data_source
{%- endset -%}

select
      person_id
    , {{ member_id_expr }} as member_id
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
