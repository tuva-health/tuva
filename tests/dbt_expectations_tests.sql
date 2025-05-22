{% macro test_expect_column_pair_values_a_to_be_greater_than_b(model, column_a, column_b, or_equal=False, row_condition=None) %}
  {% if is_fabric() %}
    {{ return(test_column_pair_values_a_greater_than_b_fabric(model, column_a, column_b, or_equal, row_condition)) }}
  {% else %}
    {{ return(dbt_expectations.test_expect_column_pair_values_a_to_be_greater_than_b(model, column_a, column_b, or_equal, row_condition)) }}
  {% endif %}
{% endmacro %}

{# Fabric implementation for column pair values A > B #}
{% macro test_column_pair_values_a_greater_than_b_fabric(model, column_a, column_b, or_equal, row_condition) %}
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
  WHERE NOT ({{ column_a }} {{ operator }} {{ column_b }})
{% endmacro %}

{% macro test_expect_column_values_to_be_between(model, column_name, min_value=None, max_value=None, strictly=False, row_condition=None) %}
  {% if is_fabric() %}
    {{ return(test_column_values_between_fabric(model, column_name, min_value, max_value, strictly, row_condition)) }}
  {% else %}
    {{ return(dbt_expectations.test_expect_column_values_to_be_between(model, column_name, min_value, max_value, strictly, row_condition)) }}
  {% endif %}
{% endmacro %}

{% macro test_column_values_between_fabric(model, column_name, min_value, max_value, strictly, row_condition) %}
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
{% endmacro %}

{% macro test_expect_column_value_lengths_to_equal(model, column_name, value, row_condition=None) %}
  {% if is_fabric() %}
    {{ return(test_column_value_lengths_equal_fabric(model, column_name, value, row_condition)) }}
  {% else %}
    {{ return(dbt_expectations.test_expect_column_value_lengths_to_equal(model, column_name, value, row_condition)) }}
  {% endif %}
{% endmacro %}

{% macro test_column_value_lengths_equal_fabric(model, column_name, value, row_condition) %}
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
{% endmacro %}

{% macro test_expect_column_value_lengths_to_be_between(model, column_name, min_value=None, max_value=None, strictly=False, row_condition=None) %}
  {% if is_fabric() %}
    {{ return(test_column_value_lengths_between_fabric(model, column_name, min_value, max_value, strictly, row_condition)) }}
  {% else %}
    {{ return(dbt_expectations.test_expect_column_value_lengths_to_be_between(model, column_name, min_value, max_value, strictly, row_condition)) }}
  {% endif %}
{% endmacro %}

{% macro test_column_value_lengths_between_fabric(model, column_name, min_value, max_value, strictly, row_condition) %}
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
{% endmacro %}

{% macro test_expect_column_unique_value_count_to_be_between(model, column_name, min_value=None, max_value=None, group_by=None, strictly=False, row_condition=None) %}
  {% if is_fabric() %}
    {{ return(test_column_unique_value_count_between_fabric(model, column_name, min_value, max_value, group_by, strictly, row_condition)) }}
  {% else %}
    {{ return(dbt_expectations.test_expect_column_unique_value_count_to_be_between(model, column_name, min_value, max_value, group_by, strictly, row_condition)) }}
  {% endif %}
{% endmacro %}

{% macro test_column_unique_value_count_between_fabric(model, column_name, min_value, max_value, group_by, strictly, row_condition) %}
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
{% endmacro %}

{% macro test_expect_column_to_exist(model, column_name) %}
  {% if is_fabric() %}
    {{ return(test_column_exists_fabric(model, column_name)) }}
  {% else %}
    {{ return(dbt_expectations.test_expect_column_to_exist(model, column_name)) }}
  {% endif %}
{% endmacro %}

{% macro test_column_exists_fabric(model, column_name) %}
  SELECT COUNT(*) as failures
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE
    TABLE_NAME = '{{ model.name }}'
    {% if model.schema %}AND TABLE_SCHEMA = '{{ model.schema }}'{% endif %}
    AND COLUMN_NAME = '{{ column_name }}'
  HAVING COUNT(*) = 0
{% endmacro %}
