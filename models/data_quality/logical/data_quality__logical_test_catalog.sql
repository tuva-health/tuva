{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'logical_test_catalog',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{#
    Only the first UNION ALL branch carries column aliases; the rest inherit
    them positionally. With one branch per logical test this keeps the
    generated statement well inside Athena's 262,144-byte query-string limit,
    which the fully-aliased form exceeded.
#}

{% set catalog_queries = [] %}

{% for definition in dq_enabled_logical_test_manifest() %}
    {% set query %}
        select
              {{ dq_string_literal_sql(definition['test_name']) }}{{ ' as test_name' if loop.first else '' }}
            , {{ dq_string_literal_sql(definition['display_name']) }}{{ ' as display_name' if loop.first else '' }}
            , {{ dq_string_literal_sql(definition['description']) }}{{ ' as description' if loop.first else '' }}
            , {{ dq_string_literal_sql(definition['input_model_name']) }}{{ ' as input_model_name' if loop.first else '' }}
            , {{ dq_string_literal_sql(definition['input_table_name']) }}{{ ' as input_table_name' if loop.first else '' }}
            , {{ dq_string_literal_sql(definition['source_model_name']) }}{{ ' as flag_model_name' if loop.first else '' }}
            , {{ dq_string_literal_sql(definition['flag_table_name']) }}{{ ' as flag_table_name' if loop.first else '' }}
            , {{ dq_string_literal_sql(definition['flag_column_name']) }}{{ ' as flag_column_name' if loop.first else '' }}
            , {{ dq_string_literal_sql(definition['grain']) }}{{ ' as grain' if loop.first else '' }}
            , {{ dq_string_literal_sql(definition['key_columns'] | join(',')) }}{{ ' as key_columns' if loop.first else '' }}
            , {{ dq_string_literal_sql(definition['test_type']) }}{{ ' as test_type' if loop.first else '' }}
            , cast({{ definition['severity'] }} as {{ dbt.type_int() }}){{ ' as severity' if loop.first else '' }}
    {% endset %}
    {% do catalog_queries.append(query) %}
{% endfor %}

{% if catalog_queries | length > 0 %}
    select *
    from (
        {{ catalog_queries | join('\nunion all\n') }}
    ) as logical_test_catalog
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as test_name
        , cast(null as {{ dbt.type_string() }}) as display_name
        , cast(null as {{ dbt.type_string() }}) as description
        , cast(null as {{ dbt.type_string() }}) as input_model_name
        , cast(null as {{ dbt.type_string() }}) as input_table_name
        , cast(null as {{ dbt.type_string() }}) as flag_model_name
        , cast(null as {{ dbt.type_string() }}) as flag_table_name
        , cast(null as {{ dbt.type_string() }}) as flag_column_name
        , cast(null as {{ dbt.type_string() }}) as grain
        , cast(null as {{ dbt.type_string() }}) as key_columns
        , cast(null as {{ dbt.type_string() }}) as test_type
        , cast(null as {{ dbt.type_int() }}) as severity
    {{ dq_empty_result_guard_sql() }}
{% endif %}
