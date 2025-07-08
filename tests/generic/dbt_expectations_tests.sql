{% test expect_table_row_count_to_be_between(model, min_value=None, max_value=None, group_by=None, strictly=False, row_condition=None) %}
  {% if is_fabric() %}
    {%- set min_operator = '>' if strictly else '>=' -%}
    {%- set max_operator = '<' if strictly else '<=' -%}

    WITH filtered_data AS (
      SELECT COUNT(*) as row_count
      FROM {{ model }}
      {% if row_condition %}
      WHERE {{ row_condition }}
      {% endif %}
    )

    SELECT row_count
    FROM filtered_data
    WHERE
      {% if min_value is not none %}NOT (row_count {{ min_operator }} {{ min_value }}){% endif %}
      {% if min_value is not none and max_value is not none %}OR{% endif %}
      {% if max_value is not none %}NOT (row_count {{ max_operator }} {{ max_value }}){% endif %}
  {% else %}
    {{ dbt_expectations.test_expect_table_row_count_to_be_between(model, min_value, max_value, group_by, strictly, row_condition) }}
  {% endif %}
{% endtest %}

{% test expect_column_to_exist(model, column_name, column_index=None, transform="upper") %}
  {% if is_fabric() %}
    SELECT COUNT(*) as failures
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      TABLE_NAME = '{{ model.name }}'
      {% if model.schema %}AND TABLE_SCHEMA = '{{ model.schema }}'{% endif %}
      AND COLUMN_NAME = '{{ column_name }}'
    HAVING COUNT(*) = 0
  {% else %}
    {{ dbt_expectations.test_expect_column_to_exist(model, column_name, column_index, transform) }}
  {% endif %}
{% endtest %}

{% test expect_column_pair_values_A_to_be_greater_than_B(model, column_A, column_B, or_equal=False, row_condition=None) %}
  {% if is_fabric() %}
    {%- set operator = '>=' if or_equal else '>' -%}

    WITH filtered_data AS (
      SELECT *
      FROM {{ model }}
      {% if row_condition %}
      WHERE {{ row_condition }}
      {% endif %}
    )

    SELECT COUNT(*) as failures
    FROM filtered_data
    WHERE NOT ({{ column_A }} {{ operator }} {{ column_B }})
  {% else %}
    {{ dbt_expectations.test_expect_column_pair_values_A_to_be_greater_than_B(model, column_A, column_B, or_equal, row_condition) }}
  {% endif %}
{% endtest %}

{% test expect_column_values_to_be_of_type(model, column_name, column_type) %}
  {% if is_fabric() %}

    SELECT COUNT(*) as failures
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      TABLE_NAME = '{{ model.name }}'
      {% if model.schema %}AND TABLE_SCHEMA = '{{ model.schema }}'{% endif %}
      AND DATA_TYPE != '{{ column_type }}'
  {% else %}
    {{ dbt_expectations.test_expect_column_values_to_be_of_type(model, column_name, column_type) }}
  {% endif %}
{% endtest %}

{% test expect_column_values_to_be_in_type_list(model, column_name, column_type_list) %}
  {% if is_fabric() %}
    SELECT COUNT(*) as failures
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      TABLE_NAME = '{{ model.name }}'
      {% if model.schema %}AND TABLE_SCHEMA = '{{ model.schema }}'{% endif %}
      AND DATA_TYPE NOT IN (
        {% for column_type in column_type_list %}
          '{{ column_type }}'{% if not loop.last %},{% endif %}
        {% endfor %}
      )
  {% else %}
    {{ dbt_expectations.test_expect_column_values_to_be_in_type_list(model, column_name, column_type) }}
  {% endif %}
{% endtest %}

{% test expect_column_values_to_be_between(model, column_name, min_value=None, max_value=None, strictly=False, row_condition=None) %}
  {% if is_fabric() %}
    {%- set min_operator = '>' if strictly else '>=' -%}
    {%- set max_operator = '<' if strictly else '<=' -%}

    WITH filtered_data AS (
      SELECT *
      FROM {{ model }}
      {% if row_condition %}
      WHERE {{ row_condition }}
      {% endif %}
    )

    SELECT COUNT(*) as failures
    FROM filtered_data
    WHERE
      {% if min_value is not none %}NOT ({{ column_name }} {{ min_operator }} {{ min_value }}){% endif %}
      {% if min_value is not none and max_value is not none %}OR{% endif %}
      {% if max_value is not none %}NOT ({{ column_name }} {{ max_operator }} {{ max_value }}){% endif %}
  {% else %}
    {{ dbt_expectations.test_expect_column_values_to_be_between(model, column_name, min_value, max_value, strictly, row_condition) }}
  {% endif %}
{% endtest %}

{% test expect_column_value_lengths_to_equal(model, column_name, value, row_condition=None) %}
  {% if is_fabric() %}
    WITH filtered_data AS (
      SELECT *
      FROM {{ model }}
      {% if row_condition %}
      WHERE {{ row_condition }}
      {% endif %}
    )

    SELECT COUNT(*) as failures
    FROM filtered_data
    WHERE LEN({{ column_name }}) != {{ value }}
  {% else %}
    {{ dbt_expectations.test_expect_column_value_lengths_to_equal(model, column_name, value, row_condition) }}
  {% endif %}
{% endtest %}

{% test expect_column_value_lengths_to_be_between(model, column_name, min_value=None, max_value=None, strictly=False, row_condition=None) %}
  {% if is_fabric() %}
    {%- set min_operator = '>' if strictly else '>=' -%}
    {%- set max_operator = '<' if strictly else '<=' -%}

    WITH filtered_data AS (
      SELECT *
      FROM {{ model }}
      {% if row_condition %}
      WHERE {{ row_condition }}
      {% endif %}
    )

    SELECT COUNT(*) as failures
    FROM filtered_data
    WHERE
      {% if min_value is not none %}NOT (LEN({{ column_name }}) {{ min_operator }} {{ min_value }}){% endif %}
      {% if min_value is not none and max_value is not none %}OR{% endif %}
      {% if max_value is not none %}NOT (LEN({{ column_name }}) {{ max_operator }} {{ max_value }}){% endif %}
  {% else %}
    {{ dbt_expectations.test_expect_column_value_lengths_to_be_between(model, column_name, min_value, max_value, strictly, row_condition) }}
  {% endif %}
{% endtest %}

{% test expect_column_unique_value_count_to_be_between(model, column_name, min_value=None, max_value=None, group_by=None, strictly=False, row_condition=None) %}
  {% if is_fabric() %}
    {%- set min_operator = '>' if strictly else '>=' -%}
    {%- set max_operator = '<' if strictly else '<=' -%}

    WITH filtered_data AS (
      SELECT *
      FROM {{ model }}
      {% if row_condition %}
      WHERE {{ row_condition }}
      {% endif %}
    ),

    unique_values AS (
      SELECT
        {% if group_by %}
          {% for g in group_by %}
            {{ g }},
          {% endfor %}
        {% endif %}
        COUNT(DISTINCT {{ column_name }}) AS unique_value_count
      FROM filtered_data
      {% if group_by %}
      GROUP BY
        {% for g in group_by %}
          {{ g }}{% if not loop.last %},{% endif %}
        {% endfor %}
      {% endif %}
    )

    SELECT COUNT(*) as failures
    FROM unique_values
    WHERE
      {% if min_value is not none %}NOT (unique_value_count {{ min_operator }} {{ min_value }}){% endif %}
      {% if min_value is not none and max_value is not none %}OR{% endif %}
      {% if max_value is not none %}NOT (unique_value_count {{ max_operator }} {{ max_value }}){% endif %}
  {% else %}
    {{ dbt_expectations.test_expect_column_unique_value_count_to_be_between(model, column_name, min_value, max_value, group_by, strictly, row_condition) }}
  {% endif %}
{% endtest %}

{% test expect_column_values_to_match_regex(model, column_name, regex, row_condition=None, is_raw=False, flags=N) %}
  {% if is_fabric() %}
    WITH filtered_data AS (
      SELECT *
      FROM {{ model }}
      {% if row_condition %}
      WHERE {{ row_condition }}
      {% endif %}
    )

    SELECT COUNT(*) as failures
    FROM filtered_data
    WHERE NOT REGEXP_LIKE({{ column_name }}, {{ regex }})
  {% else %}
    {{ dbt_expectations.test_expect_column_values_to_match_regex(model, column_name, regex, row_condition, is_raw, flags) }}
  {% endif %}
{% endtest %}

{% test expect_column_values_to_match_regex_list(model, column_name, regex_list, match_on, row_condition=any, is_raw=False, flags=N) %}
  {% if is_fabric() %}
    WITH filtered_data AS (
      SELECT *
      FROM {{ model }}
      {% if row_condition %}
      WHERE {{ row_condition }}
      {% endif %}
    )

    SELECT COUNT(*) as failures
    FROM filtered_data
    {% for regex in regex_list %}
    WHERE NOT REGEXP_LIKE({{ column_name }}, {{ regex }})
    {% if not loop.last %} AND NOT REGEXP_LIKE({{ column_name }}, {{ regex }}){% endif %}
    {% endfor %}

  {% else %}
    {{ dbt_expectations.test_expect_column_values_to_match_regex_list(model, column_name, regex_list, match_on, row_condition, is_raw, flags) }}
  {% endif %}
{% endtest %}
