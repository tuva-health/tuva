{{ config(
     enabled = var('enable_data_quality', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_primary_key_tests',
     tags = ['data_quality', 'dq', 'dq1', 'dq_structural'],
     materialized = 'table'
   )
}}

{% set claim_model_names = dq_claims_structural_model_names() %}

{% for model_name in claim_model_names %}
-- depends_on: {{ ref(model_name) }}
{% endfor %}

{% if execute %}
    {% set pk_queries = [] %}

    {% for model_name in claim_model_names %}
        {% set model_node = dq_find_model_node(model_name) %}

        {% if model_node is not none %}
            {% set table_name = model_name | replace('input_layer__', '') %}
            {% set relation = dq_actual_relation(model_node) %}
            {% set pk_columns = dq_expected_pk_columns(model_node) %}
            {% set pk_column_list = pk_columns | join(', ') %}
            {% set actual_column_types = {} %}
            {% set missing_pk_columns = [] %}

            {% if relation is not none %}
                {% for column in dq_actual_columns(relation) %}
                    {% do actual_column_types.update({column.name | lower: column.dtype}) %}
                {% endfor %}
                {% set source_dimension_sql = dq_source_dimension_sql(relation) %}
                {% set source_count_sql = dq_source_row_count_sql(relation) %}
                {% set source_key_expression = dq_source_key_expression_sql(relation, 'source_rows') %}

                {% for pk_column in pk_columns %}
                    {% if actual_column_types.get(pk_column) is none %}
                        {% do missing_pk_columns.append(pk_column) %}
                    {% endif %}
                {% endfor %}
            {% else %}
                {% set source_dimension_sql = dq_missing_source_dimension_sql() %}
                {% set source_count_sql %}
                    select
                          '{{ dq_source_key_sentinel() }}' as data_source_key
                        , cast(null as {{ dbt.type_int() }}) as row_count
                {% endset %}
            {% endif %}

            {% for pk_column in pk_columns %}
                {% if relation is none %}
                    {% set query %}
                        select
                              sources.data_source
                            , '{{ table_name }}' as {{ adapter.quote('table') }}
                            , '{{ pk_column }}' as {{ adapter.quote('column') }}
                            , 'not null' as test
                            , cast(null as {{ dbt.type_int() }}) as test_result
                        from (
                            {{ source_dimension_sql }}
                        ) as sources
                    {% endset %}
                {% elif actual_column_types.get(pk_column) is none %}
                    {% set query %}
                        select
                              sources.data_source
                            , '{{ table_name }}' as {{ adapter.quote('table') }}
                            , '{{ pk_column }}' as {{ adapter.quote('column') }}
                            , 'not null' as test
                            , cast(coalesce(source_counts.row_count, 0) as {{ dbt.type_int() }}) as test_result
                        from (
                            {{ source_dimension_sql }}
                        ) as sources
                        left join (
                            {{ source_count_sql }}
                        ) as source_counts
                            on sources.data_source_key = source_counts.data_source_key
                    {% endset %}
                {% else %}
                    {% set query %}
                        select
                              sources.data_source
                            , '{{ table_name }}' as {{ adapter.quote('table') }}
                            , '{{ pk_column }}' as {{ adapter.quote('column') }}
                            , 'not null' as test
                            , cast(coalesce(null_counts.test_result, 0) as {{ dbt.type_int() }}) as test_result
                        from (
                            {{ source_dimension_sql }}
                        ) as sources
                        left join (
                            select
                                  {{ source_key_expression }} as data_source_key
                                , cast(count(*) as {{ dbt.type_int() }}) as test_result
                            from {{ relation }} as source_rows
                            where source_rows.{{ quote_column(pk_column) }} is null
                            group by {{ source_key_expression }}
                        ) as null_counts
                            on sources.data_source_key = null_counts.data_source_key
                    {% endset %}
                {% endif %}

                {% do pk_queries.append(query) %}
            {% endfor %}

            {% if relation is none %}
                {% set duplicate_query %}
                    select
                          sources.data_source
                        , '{{ table_name }}' as {{ adapter.quote('table') }}
                        , '{{ pk_column_list }}' as {{ adapter.quote('column') }}
                        , 'duplicate value' as test
                        , cast(null as {{ dbt.type_int() }}) as test_result
                    from (
                        {{ source_dimension_sql }}
                    ) as sources
                {% endset %}
            {% elif missing_pk_columns | length > 0 %}
                {% set duplicate_query %}
                    select
                          sources.data_source
                        , '{{ table_name }}' as {{ adapter.quote('table') }}
                        , '{{ pk_column_list }}' as {{ adapter.quote('column') }}
                        , 'duplicate value' as test
                        , cast(coalesce(source_counts.row_count, 0) as {{ dbt.type_int() }}) as test_result
                    from (
                        {{ source_dimension_sql }}
                    ) as sources
                    left join (
                        {{ source_count_sql }}
                    ) as source_counts
                        on sources.data_source_key = source_counts.data_source_key
                {% endset %}
            {% else %}
                {% set duplicate_pk_columns = [] %}

                {% for pk_column in pk_columns %}
                    {% if pk_column != 'data_source' %}
                        {% do duplicate_pk_columns.append(pk_column) %}
                    {% endif %}
                {% endfor %}

                {% if duplicate_pk_columns | length == 0 %}
                    {% set duplicate_pk_columns = pk_columns %}
                {% endif %}

                {% set duplicate_query %}
                    select
                          sources.data_source
                        , '{{ table_name }}' as {{ adapter.quote('table') }}
                        , '{{ pk_column_list }}' as {{ adapter.quote('column') }}
                        , 'duplicate value' as test
                        , cast(coalesce(source_counts.row_count, 0) - coalesce(distinct_counts.distinct_row_count, 0) as {{ dbt.type_int() }}) as test_result
                    from (
                        {{ source_dimension_sql }}
                    ) as sources
                    left join (
                        {{ source_count_sql }}
                    ) as source_counts
                        on sources.data_source_key = source_counts.data_source_key
                    left join (
                        select
                              distinct_rows.data_source_key
                            , cast(count(*) as {{ dbt.type_int() }}) as distinct_row_count
                        from (
                            select
                                  {{ source_key_expression }} as data_source_key
                                {% for pk_column in duplicate_pk_columns %}
                                , source_rows.{{ quote_column(pk_column) }}
                                {% endfor %}
                            from {{ relation }} as source_rows
                            group by
                                  {{ source_key_expression }}
                                {% for pk_column in duplicate_pk_columns %}
                                , source_rows.{{ quote_column(pk_column) }}
                                {% endfor %}
                        ) as distinct_rows
                        group by distinct_rows.data_source_key
                    ) as distinct_counts
                        on sources.data_source_key = distinct_counts.data_source_key
                {% endset %}
            {% endif %}

            {% do pk_queries.append(duplicate_query) %}
        {% endif %}
    {% endfor %}

    select *
    from (
        {{ pk_queries | join('\nunion all\n') }}
    ) as structural_primary_key_tests
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as {{ adapter.quote('table') }}
        , cast(null as {{ dbt.type_string() }}) as {{ adapter.quote('column') }}
        , cast(null as {{ dbt.type_string() }}) as test
        , cast(null as {{ dbt.type_int() }}) as test_result
    {{ dq_empty_result_guard_sql() }}
{% endif %}
