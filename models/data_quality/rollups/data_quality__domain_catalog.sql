{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'domain_catalog',
     tags = ['data_quality', 'dq_rollup'],
     materialized = 'table'
   )
}}

{% set domain_queries = [] %}

{% for row in dq_domain_catalog_rows() %}
    {% set query %}
        select
              {{ dq_string_literal_sql(row['domain_group_key']) }} as domain_group_key
            , {{ dq_string_literal_sql(row['domain_group_name']) }} as domain_group_name
            , {{ dq_string_literal_sql(row['component_key']) }} as component_key
            , {{ dq_string_literal_sql(row['component_name']) }} as component_name
            , cast({{ row['display_order'] }} as {{ dbt.type_int() }}) as display_order
    {% endset %}
    {% do domain_queries.append(query) %}
{% endfor %}

select *
from (
    {{ domain_queries | join('\nunion all\n') }}
) as domain_catalog
