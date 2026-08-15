{{ config(
     enabled = (var('enable_data_quality', false) | string | lower) == 'true'
   )
}}

{% set requirement_queries = [] %}

{% for row in dq_domain_input_requirement_rows() %}
    {% if row['domain_group_key'] is not none and row['component_key'] is not none and row['input_table_name'] is not none %}
        {% set query %}
            select
                  {{ dq_string_literal_sql(row['domain_group_key']) }} as domain_group_key
                , {{ dq_string_literal_sql(row['component_key']) }} as component_key
                , {{ dq_string_literal_sql(row['input_table_name']) }} as input_table_name
                , {{ dq_string_literal_sql(row.get('input_column_name')) }} as input_column_name
                , {{ dq_string_literal_sql(row.get('requirement_level', 'required')) }} as requirement_level
                , {{ dq_string_literal_sql(row.get('source', 'unknown')) }} as requirement_source
        {% endset %}
        {% do requirement_queries.append(query) %}
    {% endif %}
{% endfor %}

{% if requirement_queries | length > 0 %}
    select distinct *
    from (
        {{ requirement_queries | join('\nunion all\n') }}
    ) as catalog_domain_requirement
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as domain_group_key
        , cast(null as {{ dbt.type_string() }}) as component_key
        , cast(null as {{ dbt.type_string() }}) as input_table_name
        , cast(null as {{ dbt.type_string() }}) as input_column_name
        , cast(null as {{ dbt.type_string() }}) as requirement_level
        , cast(null as {{ dbt.type_string() }}) as requirement_source
    {{ dq_empty_result_guard_sql() }}
{% endif %}
