{{ config(
     enabled = the_tuva_project.tuva_boolean_var('data_quality_enabled', false),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_source_populations',
     tags = ['data_quality', 'dq_structural'],
     materialized = 'table'
   )
}}

{% set input_layer_model_names = dq_enabled_input_layer_model_names() %}

{% for model_name in input_layer_model_names %}
-- depends_on: {{ ref(model_name) }}
{% endfor %}

{% if execute %}
    {% set population_queries = [] %}

    {% for model_node in dq_expected_input_layer_models() %}
        {% set table_name = model_node.name | replace('input_layer__', '') %}
        {% set input_layer_domain = dq_input_layer_domain_name(model_node.name) %}
        {% set relation = dq_required_actual_relation(model_node) %}
        {% set actual_columns = dq_actual_columns(relation) %}
        {% set actual_data_source = dq_actual_column(actual_columns, 'data_source', relation) %}

        {% if actual_data_source is not none and dq_type_family(actual_data_source.dtype) == 'string' %}
            {% set data_source_expression = 'source_table.' ~ adapter.quote(actual_data_source.name) %}
        {% else %}
            {% set data_source_expression = 'null' %}
        {% endif %}

        {% set source_key_expression = dq_structural_source_key_sql('source_rows.data_source') %}

        {% set query %}
            select
                  '{{ input_layer_domain }}' as input_layer_domain
                , source_rows.data_source
                , {{ source_key_expression }} as data_source_key
                , '{{ table_name }}' as table_name
                , '{{ model_node.name }}' as model_name
                , cast(count(source_rows._dq_row_present) as {{ dbt.type_bigint() }}) as row_count
            from (
                {{ dq_empty_row_sql() }}
            ) as empty_source
            left outer join (
                select
                      cast({{ data_source_expression }} as {{ dbt.type_string() }}) as data_source
                    , 1 as _dq_row_present
                from {{ relation }} as source_table
            ) as source_rows
                on 1 = 1
            group by
                  source_rows.data_source
                , {{ source_key_expression }}
        {% endset %}

        {% do population_queries.append(query) %}
    {% endfor %}

    {% if population_queries | length > 0 %}
        select *
        from (
            {{ population_queries | join('\nunion all\n') }}
        ) as structural_source_populations
    {% else %}
        select
              cast(null as {{ dbt.type_string() }}) as input_layer_domain
            , cast(null as {{ dbt.type_string() }}) as data_source
            , cast(null as {{ dbt.type_string() }}) as data_source_key
            , cast(null as {{ dbt.type_string() }}) as table_name
            , cast(null as {{ dbt.type_string() }}) as model_name
            , cast(null as {{ dbt.type_bigint() }}) as row_count
        {{ dq_empty_result_guard_sql() }}
    {% endif %}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as input_layer_domain
        , cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as data_source_key
        , cast(null as {{ dbt.type_string() }}) as table_name
        , cast(null as {{ dbt.type_string() }}) as model_name
        , cast(null as {{ dbt.type_bigint() }}) as row_count
    {{ dq_empty_result_guard_sql() }}
{% endif %}
