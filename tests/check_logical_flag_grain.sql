{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     severity = 'error',
     tags = ['data_quality', 'dq_logical']
   )
}}

{% set definitions_by_model = {} %}
{% set duplicate_grain_queries = [] %}

{% for definition in dq_enabled_logical_test_manifest() %}
    {% set source_model_name = definition['source_model_name'] %}
    {% if definitions_by_model.get(source_model_name) is none %}
        {% do definitions_by_model.update({source_model_name: definition}) %}
    {% elif definitions_by_model[source_model_name]['key_columns'] != definition['key_columns'] %}
        {{ exceptions.raise_compiler_error(
            "Logical flag model " ~ source_model_name ~ " has inconsistent key-column metadata."
        ) }}
    {% endif %}
{% endfor %}

{% for source_model_name, definition in definitions_by_model.items() %}
    {% set quoted_key_columns = [] %}
    {% for key_column in definition['key_columns'] %}
        {% do quoted_key_columns.append(quote_column(key_column)) %}
    {% endfor %}
    {% set query %}
        select
            {{ dq_string_literal_sql(source_model_name) }} as flag_model_name
        from {{ ref(source_model_name) }}
        group by {{ quoted_key_columns | join(', ') }}
        having count(*) > 1
    {% endset %}
    {% do duplicate_grain_queries.append(query) %}
{% endfor %}

{% if duplicate_grain_queries | length > 0 %}
    {{ duplicate_grain_queries | join('\nunion all\n') }}
{% else %}
    select
        cast(null as {{ dbt.type_string() }}) as flag_model_name
    {{ dq_empty_result_guard_sql() }}
{% endif %}
