{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_primary_key_tests',
     tags = ['data_quality', 'dq_structural'],
     materialized = 'table'
   )
}}

{% set input_layer_model_names = dq_enabled_input_layer_model_names() %}
{% set evaluation_scope = ref('data_quality__structural_evaluation_scope') %}

{% for model_name in input_layer_model_names %}
-- depends_on: {{ ref(model_name) }}
{% endfor %}

{% if execute %}
    {% set model_queries = [] %}

    {% for model_name in input_layer_model_names %}
        {% set model_node = dq_find_model_node(model_name) %}

        {% if model_node is not none %}
            {% set table_name = model_name | replace('input_layer__', '') %}
            {% set relation = dq_required_actual_relation(model_node) %}
            {% set actual_columns = dq_actual_columns(relation) %}
            {% set pk_columns = dq_expected_pk_columns(model_node) %}
            {% set pk_column_list = pk_columns | join(', ') %}
            {% set pk_metrics = [] %}
            {% set missing_pk_columns = [] %}
            {% set unsupported_pk_columns = [] %}
            {% set grouped_value_counter = namespace(value=0) %}
            {% set actual_data_source = dq_actual_column(actual_columns, 'data_source', relation) %}

            {% if actual_data_source is not none %}
                {% set source_key_expression = dq_structural_source_key_sql(
                    'source_rows.' ~ adapter.quote(actual_data_source.name)
                ) %}
            {% else %}
                {% set source_key_expression = "'" ~ dq_structural_null_source_key() ~ "'" %}
            {% endif %}

            {% for pk_column in pk_columns %}
                {% set actual_column = dq_actual_column(actual_columns, pk_column, relation) %}
                {% set grouped_alias = none %}

                {% if actual_column is none %}
                    {% do missing_pk_columns.append(pk_column) %}
                {% elif pk_column == 'data_source' and dq_type_family(actual_column.dtype) != 'string' %}
                    {% do unsupported_pk_columns.append(pk_column) %}
                {% elif dq_type_family(actual_column.dtype) not in dq_supported_type_families() %}
                    {% do unsupported_pk_columns.append(pk_column) %}
                {% elif pk_column != 'data_source' %}
                    {% set grouped_value_counter.value = grouped_value_counter.value + 1 %}
                    {% set grouped_alias = 'pk_value_' ~ grouped_value_counter.value %}
                {% endif %}

                {% do pk_metrics.append({
                    'column_name': pk_column,
                    'actual_column_name': actual_column.name if actual_column is not none else none,
                    'metric_alias': 'null_count_' ~ loop.index,
                    'grouped_alias': grouped_alias,
                    'is_present': actual_column is not none
                }) %}
            {% endfor %}

            {% set definition_queries = [] %}
            {% for metric in pk_metrics %}
                {% set definition_query %}
                    select
                          'null_{{ loop.index }}' as test_key
                        , '{{ metric["column_name"] | replace("'", "''") }}' as column_name
                        , 'not null' as test
                        , cast({{ 1 if missing_pk_columns | length == 0 and unsupported_pk_columns | length == 0 else 0 }} as {{ dbt.type_int() }}) as is_evaluable
                {% endset %}
                {% do definition_queries.append(definition_query) %}
            {% endfor %}

            {% set duplicate_definition %}
                select
                      'duplicate' as test_key
                    , '{{ pk_column_list | replace("'", "''") }}' as column_name
                    , 'duplicate value' as test
                    , cast({{ 1 if missing_pk_columns | length == 0 and unsupported_pk_columns | length == 0 else 0 }} as {{ dbt.type_int() }}) as is_evaluable
            {% endset %}
            {% do definition_queries.append(duplicate_definition) %}

            {% set grouped_key_sql %}
                select
                      {{ source_key_expression }} as data_source_key
                    {% for metric in pk_metrics if metric['grouped_alias'] is not none %}
                    , source_rows.{{ adapter.quote(metric['actual_column_name']) }} as {{ metric['grouped_alias'] }}
                    {% endfor %}
                    , cast(count(*) as {{ dbt.type_bigint() }}) as group_record_count
                from {{ relation }} as source_rows
                group by
                      {{ source_key_expression }}
                    {% for metric in pk_metrics if metric['grouped_alias'] is not none %}
                    , source_rows.{{ adapter.quote(metric['actual_column_name']) }}
                    {% endfor %}
            {% endset %}

            {% set source_metrics_sql %}
                select
                      grouped_keys.data_source_key
                    {% for metric in pk_metrics %}
                    , {% if not metric['is_present'] %}
                        cast(null as {{ dbt.type_bigint() }})
                      {% elif metric['column_name'] == 'data_source' %}
                        cast(sum(
                            case
                              when grouped_keys.data_source_key = '{{ dq_structural_null_source_key() }}'
                              then grouped_keys.group_record_count
                              else 0
                            end
                        ) as {{ dbt.type_bigint() }})
                      {% else %}
                        cast(sum(
                            case
                              when grouped_keys.{{ metric['grouped_alias'] }} is null
                              then grouped_keys.group_record_count
                              else 0
                            end
                        ) as {{ dbt.type_bigint() }})
                      {% endif %} as {{ metric['metric_alias'] }}
                    {% endfor %}
                    , {% if missing_pk_columns | length > 0 or unsupported_pk_columns | length > 0 %}
                        cast(null as {{ dbt.type_bigint() }})
                      {% else %}
                        cast(sum(
                            case
                              when grouped_keys.group_record_count > 1
                              then grouped_keys.group_record_count
                              else 0
                            end
                        ) as {{ dbt.type_bigint() }})
                      {% endif %} as duplicate_record_count
                from (
                    {{ grouped_key_sql }}
                ) as grouped_keys
                group by grouped_keys.data_source_key
            {% endset %}

            {% set metric_case_clauses = [] %}
            {% for metric in pk_metrics %}
                {% do metric_case_clauses.append(
                    "when 'null_" ~ loop.index ~ "' then source_metrics." ~ metric['metric_alias']
                ) %}
            {% endfor %}
            {% do metric_case_clauses.append("when 'duplicate' then source_metrics.duplicate_record_count") %}

            {% if missing_pk_columns | length > 0 or unsupported_pk_columns | length > 0 %}
                {% set model_query %}
                    select
                          sources.data_source
                        , '{{ table_name }}' as input_table_name
                        , test_definitions.column_name
                        , test_definitions.test
                        , cast(null as {{ dbt.type_bigint() }}) as test_result
                    from (
                        select
                              data_source
                            , data_source_key
                        from {{ evaluation_scope }}
                        where model_name = '{{ model_name }}'
                    ) as sources
                    cross join (
                        {{ definition_queries | join('\nunion all\n') }}
                    ) as test_definitions
                {% endset %}
            {% else %}
                {% set model_query %}
                select
                      sources.data_source
                    , '{{ table_name }}' as input_table_name
                    , test_definitions.column_name
                    , test_definitions.test
                    , case
                        when test_definitions.is_evaluable = 0
                        then cast(null as {{ dbt.type_bigint() }})
                        else cast(coalesce(
                            case test_definitions.test_key
                              {{ metric_case_clauses | join('\n                              ') }}
                            end,
                            0
                        ) as {{ dbt.type_bigint() }})
                      end as test_result
                from (
                    select
                          data_source
                        , data_source_key
                    from {{ evaluation_scope }}
                    where model_name = '{{ model_name }}'
                ) as sources
                cross join (
                    {{ definition_queries | join('\nunion all\n') }}
                ) as test_definitions
                left join (
                    {{ source_metrics_sql }}
                ) as source_metrics
                    on sources.data_source_key = source_metrics.data_source_key
                {% endset %}
            {% endif %}

            {% do model_queries.append(model_query) %}
        {% endif %}
    {% endfor %}

    {% if model_queries | length > 0 %}
        select *
        from (
            {{ model_queries | join('\nunion all\n') }}
        ) as structural_primary_key_tests
    {% else %}
        select
              cast(null as {{ dbt.type_string() }}) as data_source
            , cast(null as {{ dbt.type_string() }}) as input_table_name
            , cast(null as {{ dbt.type_string() }}) as column_name
            , cast(null as {{ dbt.type_string() }}) as test
            , cast(null as {{ dbt.type_bigint() }}) as test_result
        {{ dq_empty_result_guard_sql() }}
    {% endif %}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as input_table_name
        , cast(null as {{ dbt.type_string() }}) as column_name
        , cast(null as {{ dbt.type_string() }}) as test
        , cast(null as {{ dbt.type_bigint() }}) as test_result
    {{ dq_empty_result_guard_sql() }}
{% endif %}
