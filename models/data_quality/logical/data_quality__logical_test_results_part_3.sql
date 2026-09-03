{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = '_logical_test_results_part_3',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{#
    Implementation detail, not a consumer contract. One slice of the logical
    test manifest, split on whole flag models so per-model aggregates stay
    correct, and kept small enough that the generated statement stays inside
    Athena's 262,144-byte query-string limit.
    data_quality__logical_test_results unions every part.
#}

{% set definitions_by_model = {} %}

{% for definition in the_tuva_project.dq_enabled_logical_test_manifest_chunk_by_model(2) %}
    {% set source_model_name = definition['source_model_name'] %}
    {% if definitions_by_model.get(source_model_name) is none %}
        {% do definitions_by_model.update({source_model_name: []}) %}
    {% endif %}
    {% do definitions_by_model[source_model_name].append(definition) %}
{% endfor %}

{% set aggregate_ctes = [] %}
{% set test_definition_ctes = [] %}
{% set result_queries = [] %}

{% for source_model_name, model_definitions in definitions_by_model.items() %}
    {% set model_index = loop.index %}
    {% set aggregate_columns = [] %}
    {% set test_definition_queries = [] %}
    {% set tested_count_cases = [] %}
    {% set failed_count_cases = [] %}
    {% set not_applicable_count_cases = [] %}

    {% for definition in model_definitions %}
        {% set test_ordinal = loop.index %}
        {% set flag_column = quote_column(definition['flag_column_name']) %}

        {% do aggregate_columns.append(
            "cast(sum(cast(case when " ~ flag_column ~ " in (0, 1) then 1 else 0 end as "
            ~ dbt.type_bigint() ~ ")) as " ~ dbt.type_bigint() ~ ") as tested_count_" ~ test_ordinal
        ) %}
        {% do aggregate_columns.append(
            "cast(sum(cast(coalesce(" ~ flag_column ~ ", 0) as " ~ dbt.type_bigint() ~ ")) as "
            ~ dbt.type_bigint() ~ ") as failed_count_" ~ test_ordinal
        ) %}
        {% do aggregate_columns.append(
            "cast(sum(cast(case when " ~ flag_column ~ " is null then 1 else 0 end as "
            ~ dbt.type_bigint() ~ ")) as " ~ dbt.type_bigint() ~ ") as not_applicable_count_" ~ test_ordinal
        ) %}

        {% set test_definition_query %}
            select
                  cast({{ test_ordinal }} as {{ dbt.type_int() }}) as test_ordinal
                , {{ dq_string_literal_sql(definition['input_table_name']) }} as input_table_name
                , {{ dq_string_literal_sql(definition['test_name']) }} as test_name
                , {{ dq_string_literal_sql(definition['display_name']) }} as display_name
                , {{ dq_string_literal_sql(definition['description']) }} as description
                , {{ dq_string_literal_sql(definition['grain']) }} as grain
                , {{ dq_string_literal_sql(definition['flag_table_name']) }} as flag_table_name
                , {{ dq_string_literal_sql(definition['flag_column_name']) }} as flag_column_name
                , {{ dq_string_literal_sql(definition['test_type']) }} as test_type
                , cast({{ definition['severity'] }} as {{ dbt.type_int() }}) as severity
        {% endset %}
        {% do test_definition_queries.append(test_definition_query | trim) %}
        {% do tested_count_cases.append(
            "when " ~ test_ordinal ~ " then flag_aggregates.tested_count_" ~ test_ordinal
        ) %}
        {% do failed_count_cases.append(
            "when " ~ test_ordinal ~ " then flag_aggregates.failed_count_" ~ test_ordinal
        ) %}
        {% do not_applicable_count_cases.append(
            "when " ~ test_ordinal ~ " then flag_aggregates.not_applicable_count_" ~ test_ordinal
        ) %}
    {% endfor %}

    {% set aggregate_cte %}
        logical_model_{{ model_index }}_aggregates as (
            select
                  cast(data_source as {{ dbt.type_string() }}) as data_source
                , cast(
                    sum(cast(1 as {{ dbt.type_bigint() }}))
                    as {{ dbt.type_bigint() }}
                  ) as total_row_count
                , {{ aggregate_columns | join('\n                , ') }}
            from {{ ref(source_model_name) }}
            group by cast(data_source as {{ dbt.type_string() }})
        )
    {% endset %}
    {% do aggregate_ctes.append(aggregate_cte | trim) %}

    {% set test_definition_cte %}
        logical_model_{{ model_index }}_tests as (
            {{ test_definition_queries | join('\n            union all\n') }}
        )
    {% endset %}
    {% do test_definition_ctes.append(test_definition_cte | trim) %}

    {% set result_query %}
        select
              flag_aggregates.data_source
            , test_definitions.input_table_name
            , test_definitions.test_name
            , test_definitions.display_name
            , test_definitions.description
            , test_definitions.grain
            , test_definitions.flag_table_name
            , test_definitions.flag_column_name
            , test_definitions.test_type
            , test_definitions.severity
            , flag_aggregates.total_row_count
            , cast(
                case test_definitions.test_ordinal
                    {{ tested_count_cases | join('\n                    ') }}
                end
              as {{ dbt.type_bigint() }}) as tested_count
            , cast(
                case test_definitions.test_ordinal
                    {{ failed_count_cases | join('\n                    ') }}
                end
              as {{ dbt.type_bigint() }}) as failed_count
            , cast(
                case test_definitions.test_ordinal
                    {{ tested_count_cases | join('\n                    ') }}
                end
                - case test_definitions.test_ordinal
                    {{ failed_count_cases | join('\n                    ') }}
                end
              as {{ dbt.type_bigint() }}) as passed_count
            , cast(
                case test_definitions.test_ordinal
                    {{ not_applicable_count_cases | join('\n                    ') }}
                end
              as {{ dbt.type_bigint() }}) as not_applicable_count
        from logical_model_{{ model_index }}_aggregates as flag_aggregates
        cross join logical_model_{{ model_index }}_tests as test_definitions
    {% endset %}
    {% do result_queries.append(result_query | trim) %}
{% endfor %}

{% if result_queries | length > 0 %}
    with
    {{ (aggregate_ctes + test_definition_ctes) | join('\n    , ') }}

    select *
    from (
        {{ result_queries | join('\n        union all\n') }}
    ) as logical_test_results_part
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as input_table_name
        , cast(null as {{ dbt.type_string() }}) as test_name
        , cast(null as {{ dbt.type_string() }}) as display_name
        , cast(null as {{ dbt.type_string() }}) as description
        , cast(null as {{ dbt.type_string() }}) as grain
        , cast(null as {{ dbt.type_string() }}) as flag_table_name
        , cast(null as {{ dbt.type_string() }}) as flag_column_name
        , cast(null as {{ dbt.type_string() }}) as test_type
        , cast(null as {{ dbt.type_int() }}) as severity
        , cast(null as {{ dbt.type_bigint() }}) as total_row_count
        , cast(null as {{ dbt.type_bigint() }}) as tested_count
        , cast(null as {{ dbt.type_bigint() }}) as failed_count
        , cast(null as {{ dbt.type_bigint() }}) as passed_count
        , cast(null as {{ dbt.type_bigint() }}) as not_applicable_count
    {{ dq_empty_result_guard_sql() }}
{% endif %}
