{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and (the_tuva_project.tuva_boolean_var('enable_data_quality_failure_keys', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = '_logical_failure_keys_part_3',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{#
    Implementation detail, not a consumer contract. One slice of the logical
    test manifest, kept small enough that the generated statement stays inside
    Athena's 262,144-byte query-string limit. data_quality__logical_failure_keys
    unions every part.
#}

{% set key_queries = [] %}
{% set string_type = dbt.type_string() %}

{#
  percent_escaped_v1 encodes key components in key_columns order. Null is N.
  A non-null value is V plus its string value after replacing % with %25 and
  then | with %7C; therefore an empty string is V. Components are joined by |.
  Decode %7C before %25 so escaped percent sequences remain unambiguous.
#}
{% for definition in the_tuva_project.dq_enabled_logical_test_manifest_chunk(2) %}
    {% set key_value_parts = [] %}
    {% for key_column in definition['key_columns'] %}
        {% set qualified_key_column = "flags." ~ quote_column(key_column) %}
        {% set string_key_value = "cast(" ~ qualified_key_column ~ " as " ~ string_type ~ ")" %}
        {% set escaped_key_value = "replace(replace(" ~ string_key_value ~ ", '%', '%25'), '|', '%7C')" %}
        {% set encoded_component =
            "case when " ~ qualified_key_column ~ " is null then 'N' else "
            ~ concat_custom(["'V'", escaped_key_value])
            ~ " end"
        %}
        {% do key_value_parts.append(encoded_component) %}
        {% if not loop.last %}
            {% do key_value_parts.append("'|'") %}
        {% endif %}
    {% endfor %}

    {% set query %}
        select
              cast(flags.data_source as {{ string_type }}) as data_source
            , {{ dq_string_literal_sql(definition['input_table_name']) }} as input_table_name
            , {{ dq_string_literal_sql(definition['test_name']) }} as test_name
            , {{ dq_string_literal_sql(definition['grain']) }} as grain
            , {{ dq_string_literal_sql(definition['key_columns'] | join(',')) }} as key_columns
            , 'percent_escaped_v1' as key_values_format
            , {{ concat_custom(key_value_parts) }} as key_values
        from {{ ref(definition['source_model_name']) }} as flags
        where flags.{{ quote_column(definition['flag_column_name']) }} = 1
    {% endset %}
    {% do key_queries.append(query) %}
{% endfor %}

{% if key_queries | length > 0 %}
    select *
    from (
        {{ key_queries | join('\nunion all\n') }}
    ) as logical_failure_keys_part
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as input_table_name
        , cast(null as {{ dbt.type_string() }}) as test_name
        , cast(null as {{ dbt.type_string() }}) as grain
        , cast(null as {{ dbt.type_string() }}) as key_columns
        , cast(null as {{ dbt.type_string() }}) as key_values_format
        , cast(null as {{ dbt.type_string() }}) as key_values
    {{ dq_empty_result_guard_sql() }}
{% endif %}
