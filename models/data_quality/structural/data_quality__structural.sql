{{ config(
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural',
     tags = ['data_quality', 'dqi', 'dq1', 'dq_structural'],
     materialized = 'table'
   )
}}

{% set structural_dependency_names = [] %}

{% if var('clinical_enabled', false) | as_bool %}
  {% do structural_dependency_names.extend([
      'input_layer__appointment',
      'input_layer__condition',
      'input_layer__encounter',
      'input_layer__immunization',
      'input_layer__lab_result',
      'input_layer__location',
      'input_layer__medication',
      'input_layer__observation',
      'input_layer__patient',
      'input_layer__practitioner',
      'input_layer__procedure'
  ]) %}
{% endif %}

{% if var('claims_enabled', false) | as_bool %}
  {% do structural_dependency_names.extend([
      'input_layer__eligibility',
      'input_layer__medical_claim',
      'input_layer__pharmacy_claim'
  ]) %}
{% endif %}

{% if (var('provider_attribution_enabled', false) and var('claims_enabled', false)) | as_bool %}
  {% do structural_dependency_names.append('input_layer__provider_attribution') %}
{% endif %}

{% for dependency_name in structural_dependency_names %}
-- depends_on: {{ ref(dependency_name) }}
{% endfor %}

{% if execute %}
    {% set expected_models = dq_expected_input_layer_models() %}
    {% set structural_queries = [] %}

    {% for model_node in expected_models %}
        {% set table_name = model_node.name | replace('input_layer__', '') %}
        {% set relation = dq_actual_relation(model_node) %}

        {% if relation is none %}
            {% set query %}
                select
                      cast(null as {{ dbt.type_string() }}) as data_source
                    , '{{ table_name }}' as table_name
                    , 'fail' as table_exists
                    , 'fail' as columns_exist
                    , 'fail' as data_types
                    , 'fail' as primary_keys
                    , cast(null as {{ dbt.type_int() }}) as row_count
            {% endset %}
        {% else %}
            {% set expected_columns = dq_expected_columns(model_node) %}
            {% set actual_columns = dq_actual_columns(relation) %}
            {% set actual_column_types = {} %}
            {% set status = namespace(columns_exist='pass', data_types='pass', pk_columns_exist=true) %}

            {% for column in actual_columns %}
                {% do actual_column_types.update({column.name | lower: column.dtype}) %}
            {% endfor %}

            {% for expected_column in expected_columns %}
                {% set expected_name = expected_column['name'] %}
                {% set actual_type = actual_column_types.get(expected_name) %}

                {% if actual_type is none %}
                    {% set status.columns_exist = 'fail' %}
                    {% set status.data_types = 'fail' %}
                {% elif expected_column['data_type'] is not none
                        and not dq_type_families_match(expected_column['data_type'], actual_type) %}
                    {% set status.data_types = 'fail' %}
                {% endif %}
            {% endfor %}

            {% set pk_columns = dq_expected_pk_columns(model_node) %}

            {% for pk_column in pk_columns %}
                {% if actual_column_types.get(pk_column) is none %}
                    {% set status.pk_columns_exist = false %}
                {% endif %}
            {% endfor %}

            {% if pk_columns | length == 0 or not status.pk_columns_exist %}
                {% set query %}
                    select
                          sources.data_source
                        , '{{ table_name }}' as table_name
                        , 'pass' as table_exists
                        , '{{ status.columns_exist }}' as columns_exist
                        , '{{ status.data_types }}' as data_types
                        , 'fail' as primary_keys
                        , cast(coalesce(source_counts.row_count, 0) as {{ dbt.type_int() }}) as row_count
                    from (
                        {{ dq_source_dimension_sql(relation) }}
                    ) as sources
                    left join (
                        {{ dq_source_row_count_sql(relation) }}
                    ) as source_counts
                        on sources.data_source_key = source_counts.data_source_key
                {% endset %}
            {% else %}
                {% set query %}
                    select
                          sources.data_source
                        , '{{ table_name }}' as table_name
                        , 'pass' as table_exists
                        , '{{ status.columns_exist }}' as columns_exist
                        , '{{ status.data_types }}' as data_types
                        , case
                            when coalesce(pk_nulls.null_pk_count, 0) = 0
                             and coalesce(pk_duplicates.duplicate_pk_count, 0) = 0
                            then 'pass'
                            else 'fail'
                          end as primary_keys
                        , cast(coalesce(source_counts.row_count, 0) as {{ dbt.type_int() }}) as row_count
                    from (
                        {{ dq_source_dimension_sql(relation) }}
                    ) as sources
                    left join (
                        {{ dq_source_row_count_sql(relation) }}
                    ) as source_counts
                        on sources.data_source_key = source_counts.data_source_key
                    left join (
                        {{ dq_pk_null_count_sql(relation, pk_columns) }}
                    ) as pk_nulls
                        on sources.data_source_key = pk_nulls.data_source_key
                    left join (
                        {{ dq_duplicate_pk_count_sql(relation, pk_columns) }}
                    ) as pk_duplicates
                        on sources.data_source_key = pk_duplicates.data_source_key
                {% endset %}
            {% endif %}
        {% endif %}

        {% do structural_queries.append(query) %}
    {% endfor %}

    {{ structural_queries | join('\nunion all\n') }}
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as table_name
        , cast(null as {{ dbt.type_string() }}) as table_exists
        , cast(null as {{ dbt.type_string() }}) as columns_exist
        , cast(null as {{ dbt.type_string() }}) as data_types
        , cast(null as {{ dbt.type_string() }}) as primary_keys
        , cast(null as {{ dbt.type_int() }}) as row_count
    where 1 = 0
{% endif %}
