{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_actual_columns',
     tags = ['data_quality', 'dq', 'dq1', 'dq_structural'],
     materialized = 'table'
   )
}}

{% set input_layer_model_names = dq_enabled_input_layer_model_names() %}

{% for model_name in input_layer_model_names %}
-- depends_on: {{ ref(model_name) }}
{% endfor %}

{% if execute %}
    {% set actual_queries = [] %}

    {% for model_node in dq_expected_input_layer_models() %}
        {% set table_name = model_node.name | replace('input_layer__', '') %}
        {% set input_layer_domain = dq_input_layer_domain_name(model_node.name) %}
        {% set relation = dq_required_actual_relation(model_node) %}
        {% set actual_columns = dq_actual_columns(relation) %}
        {% set physical_names_by_normalized_name = {} %}

        {% for column in actual_columns %}
            {% set normalized_column_name = column.name | lower %}

            {% if normalized_column_name in physical_names_by_normalized_name %}
                {{ exceptions.raise_compiler_error(
                    "Structural Data Quality found multiple physical columns in Input Layer Wrapper '"
                    ~ model_node.name
                    ~ "' that normalize to '"
                    ~ normalized_column_name
                    ~ "': '"
                    ~ physical_names_by_normalized_name[normalized_column_name]
                    ~ "' and '"
                    ~ column.name
                    ~ "'. Structural column matching is case-insensitive, so the columns are ambiguous."
                ) }}
            {% endif %}

            {% do physical_names_by_normalized_name.update({normalized_column_name: column.name}) %}
        {% endfor %}

        {% for column in actual_columns %}
            {% set query %}
                select
                      '{{ input_layer_domain }}' as input_layer_domain
                    , '{{ table_name }}' as table_name
                    , '{{ model_node.name }}' as model_name
                    , '{{ column.name | lower | replace("'", "''") }}' as column_name
                    , '{{ column.name | replace("'", "''") }}' as actual_column_name
                    , '{{ column.dtype | replace("'", "''") }}' as actual_data_type
                    , '{{ dq_type_family(column.dtype) }}' as actual_type_family
            {% endset %}

            {% do actual_queries.append(query) %}
        {% endfor %}
    {% endfor %}

    {% if actual_queries | length > 0 %}
        select *
        from (
            {{ actual_queries | join('\nunion all\n') }}
        ) as structural_actual_columns
    {% else %}
        select
              cast(null as {{ dbt.type_string() }}) as input_layer_domain
            , cast(null as {{ dbt.type_string() }}) as table_name
            , cast(null as {{ dbt.type_string() }}) as model_name
            , cast(null as {{ dbt.type_string() }}) as column_name
            , cast(null as {{ dbt.type_string() }}) as actual_column_name
            , cast(null as {{ dbt.type_string() }}) as actual_data_type
            , cast(null as {{ dbt.type_string() }}) as actual_type_family
        {{ dq_empty_result_guard_sql() }}
    {% endif %}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as input_layer_domain
        , cast(null as {{ dbt.type_string() }}) as table_name
        , cast(null as {{ dbt.type_string() }}) as model_name
        , cast(null as {{ dbt.type_string() }}) as column_name
        , cast(null as {{ dbt.type_string() }}) as actual_column_name
        , cast(null as {{ dbt.type_string() }}) as actual_data_type
        , cast(null as {{ dbt.type_string() }}) as actual_type_family
    {{ dq_empty_result_guard_sql() }}
{% endif %}
