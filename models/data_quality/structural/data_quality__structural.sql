{{ config(
     enabled = var('enable_data_quality', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural',
     tags = ['data_quality', 'dq', 'dq1', 'dq_structural'],
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

-- depends_on: {{ ref('data_quality__structural_column_details') }}
-- depends_on: {{ ref('data_quality__structural_primary_key_tests') }}

{% if execute %}
    {% set expected_models = dq_expected_input_layer_models() %}
    {% set detailed_claim_model_names = dq_claims_structural_model_names() %}
    {% set structural_queries = [] %}

    {% for model_node in expected_models %}
        {% set table_name = model_node.name | replace('input_layer__', '') %}
        {% set relation = dq_actual_relation(model_node) %}

        {% if model_node.name in detailed_claim_model_names %}
            {% set pk_columns = dq_expected_pk_columns(model_node) %}

            {% if relation is none %}
                {% set source_dimension_sql = dq_missing_source_dimension_sql() %}
                {% set source_count_sql %}
                    select
                          '{{ dq_source_key_sentinel() }}' as data_source_key
                        , cast(null as {{ dbt.type_int() }}) as row_count
                {% endset %}
            {% else %}
                {% set source_dimension_sql = dq_source_dimension_sql(relation) %}
                {% set source_count_sql = dq_source_row_count_sql(relation) %}
            {% endif %}

            {% set query %}
                select
                      sources.data_source
                    , '{{ table_name }}' as table_name
                    , '{% if relation is not none %}pass{% else %}fail{% endif %}' as table_exists
                    , case
                        when coalesce(column_status.missing_column_count, 0) = 0
                        then 'pass'
                        else 'fail'
                      end as columns_exist
                    , case
                        when coalesce(column_status.bad_data_type_count, 0) = 0
                        then 'pass'
                        else 'fail'
                      end as data_types
                    , case
                        when {% if relation is not none %}1 = 1{% else %}1 = 0{% endif %}
                         and coalesce(pk_column_status.missing_pk_column_count, 0) = 0
                         and coalesce(pk_test_status.failing_test_count, 0) = 0
                        then 'pass'
                        else 'fail'
                      end as primary_keys
                    , cast(source_counts.row_count as {{ dbt.type_int() }}) as row_count
                from (
                    {{ source_dimension_sql }}
                ) as sources
                left join (
                    {{ source_count_sql }}
                ) as source_counts
                    on sources.data_source_key = source_counts.data_source_key
                left join (
                    select
                          coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                        , cast(sum(case when column_exists = 'no' then 1 else 0 end) as {{ dbt.type_int() }}) as missing_column_count
                        , cast(sum(case when data_type_correct = 'no' then 1 else 0 end) as {{ dbt.type_int() }}) as bad_data_type_count
                    from {{ ref('data_quality__structural_column_details') }}
                    where {{ adapter.quote('table') }} = '{{ table_name }}'
                    group by
                        coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
                ) as column_status
                    on sources.data_source_key = column_status.data_source_key
                left join (
                    select
                          coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                        , cast(sum(case when column_exists = 'no' then 1 else 0 end) as {{ dbt.type_int() }}) as missing_pk_column_count
                    from {{ ref('data_quality__structural_column_details') }}
                    where {{ adapter.quote('table') }} = '{{ table_name }}'
                      and {{ adapter.quote('column') }} in (
                          {% for pk_column in pk_columns %}
                          '{{ pk_column }}'{% if not loop.last %}, {% endif %}
                          {% endfor %}
                      )
                    group by
                        coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
                ) as pk_column_status
                    on sources.data_source_key = pk_column_status.data_source_key
                left join (
                    select
                          coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}') as data_source_key
                        , cast(sum(case when test_result is null or test_result <> 0 then 1 else 0 end) as {{ dbt.type_int() }}) as failing_test_count
                    from {{ ref('data_quality__structural_primary_key_tests') }}
                    where {{ adapter.quote('table') }} = '{{ table_name }}'
                    group by
                        coalesce(cast(data_source as {{ dbt.type_string() }}), '{{ dq_source_key_sentinel() }}')
                ) as pk_test_status
                    on sources.data_source_key = pk_test_status.data_source_key
            {% endset %}
        {% elif relation is none %}
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

    select *
    from (
        {{ structural_queries | join('\nunion all\n') }}
    ) as structural_results
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as table_name
        , cast(null as {{ dbt.type_string() }}) as table_exists
        , cast(null as {{ dbt.type_string() }}) as columns_exist
        , cast(null as {{ dbt.type_string() }}) as data_types
        , cast(null as {{ dbt.type_string() }}) as primary_keys
        , cast(null as {{ dbt.type_int() }}) as row_count
    {{ dq_empty_result_guard_sql() }}
{% endif %}
