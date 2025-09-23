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
    WHERE NOT (
      {% if min_value is not none %} row_count {{ min_operator }} {{ min_value }} {% endif %}
      {% if min_value is not none and max_value is not none %}AND{% endif %}
      {% if max_value is not none %} row_count {{ max_operator }} {{ max_value }} {% endif %}
      )

  {% else %}
    {{ dbt_expectations.test_expect_table_row_count_to_be_between(model, min_value, max_value, group_by, strictly, row_condition) }}
  {% endif %}
{% endtest %}

{% test expect_column_to_exist(model, column_name, column_index=None, transform="upper") %}
  {% if is_fabric() %}
    SELECT *
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      TABLE_NAME = '{{ model.name }}'
      {% if model.schema %}AND TABLE_SCHEMA = '{{ model.schema }}'{% endif %}
      AND COLUMN_NAME = '{{ quote_column(column_name)}}'
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

    SELECT  
      {{ column_A}}
    FROM filtered_data
    WHERE NOT ('{{ quote_column(column_A)}}' {{ operator }} '{{ quote_column(column_B)}}')
  {% else %}
    {{ dbt_expectations.test_expect_column_pair_values_A_to_be_greater_than_B(model, column_A, column_B, or_equal, row_condition) }}
  {% endif %}
{% endtest %}

{% test expect_column_values_to_be_of_type(model, column_name, column_type) %}
  {% if is_fabric() %}

    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      TABLE_NAME = '{{ model.name }}'
      {% if model.schema %}AND TABLE_SCHEMA = '{{ model.schema }}'{% endif %}
      AND DATA_TYPE != '{{ column_type }}'
      AND COLUMN_NAME = '{{ quote_column(column_name)}}'
  {% else %}
    {{ dbt_expectations.test_expect_column_values_to_be_of_type(model, column_name, column_type) }}
  {% endif %}
{% endtest %}

{% test expect_column_values_to_be_in_type_list(model, column_name, column_type_list) %}
  {% if is_fabric() %}
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE
      TABLE_NAME = '{{ model.name }}'
      {% if model.schema %}AND TABLE_SCHEMA = '{{ model.schema }}'{% endif %}
      AND DATA_TYPE NOT IN (
        {% for column_type in column_type_list %}
          '{{ column_type }}'{% if not loop.last %},{% endif %}
        {% endfor %}
      )
      AND COLUMN_NAME = '{{ quote_column(column_name)}}'
  {% else %}
    {{ dbt_expectations.test_expect_column_values_to_be_in_type_list(model, column_name, column_type_list) }}
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

    SELECT {{ column_name }}
    FROM filtered_data
    WHERE NOT (
      {% if min_value is not none %} {{ column_name }}  {{ min_operator }} {{ min_value }} {% endif %}
      {% if min_value is not none and max_value is not none %}AND{% endif %}
      {% if max_value is not none %} {{ column_name }}  {{ max_operator }} {{ max_value }} {% endif %}
    )
    --there is an internal dbt test that is run after this that calculates the failure count
    --this test evaluates the number of records, so the column of interest (column_name) is pulled
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

    SELECT {{ column_name }}
    FROM filtered_data
    WHERE LEN('{{ quote_column(column_name)}}' ) != {{ value }}
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

    SELECT {{ column_name }} 
    FROM filtered_data
    WHERE NOT (
      {% if min_value is not none %} LEN('{{ quote_column(column_name)}}' ) {{ min_operator }} {{ min_value }}{% endif %}
      {% if min_value is not none and max_value is not none %}AND{% endif %}
      {% if max_value is not none %} LEN('{{ quote_column(column_name)}}' ) {{ max_operator }} {{ max_value }}{% endif %}
    )
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
            {{ g }} AS group_by_col,
          {% endfor %}
        {% endif %}
        COUNT(DISTINCT '{{ quote_column(column_name)}}' ) AS unique_value_count
      FROM filtered_data
      {% if group_by %}
      GROUP BY
        {% for g in group_by %}
          {{ g }}{% if not loop.last %},{% endif %}
        {% endfor %}
      {% endif %}
    )

    SELECT 
      group_by_col
      , unique_value_count
    FROM unique_values
    WHERE NOT (
      {% if min_value is not none %} unique_value_count {{ min_operator }} {{ min_value }}{% endif %}
      {% if min_value is not none and max_value is not none %}AND{% endif %}
      {% if max_value is not none %} unique_value_count {{ max_operator }} {{ max_value }}{% endif %}
    )
    --there is an internal dbt test that is run after this that calculates the failure count
    --this test evaluates unique_value_count, so unique_value_count needs to be in the output
  {% else %}
    {{ dbt_expectations.test_expect_column_unique_value_count_to_be_between(model, column_name, min_value, max_value, group_by, strictly, row_condition) }}
  {% endif %}
{% endtest %}

{% test expect_column_values_to_match_regex(model, column_name, regex, pattern1, pattern2, row_condition=None, is_raw=False, flags=N) %}
  {% if is_fabric() %}
    WITH filtered_data AS (
      SELECT *
      FROM {{ model }}
      {% if row_condition %}
      WHERE {{ row_condition }}
      {% endif %}
    )

    SELECT {{ column_name }}
    FROM filtered_data
    {% if column_name == 'bill_type_code' or column_name == 'revenue_center_code'%}
    WHERE NOT (
      (LEN({{ column_name }} ) = 3 AND PATINDEX('{{ pattern_1 }}', {{ column_name }}) = 1) OR
      (LEN({{ column_name }} ) = 4 AND PATINDEX('{{ pattern_2 }}', {{ column_name }} ) = 1)
    )
    {% else %} --for admit_type_code, place_of_service_code, rendering_npi, billing_npi, facility_npi
    WHERE NOT PATINDEX('{{ pattern1 }}', {{ column_name }} ) = 1
    {% endif %}
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

    {% for col in adapter.get_columns_in_relation(model) %}
      {% if column_name == col.name and 'diagnosis_code' in col.name %} 
        SELECT {{ column_name }}
        FROM filtered_data
        WHERE NOT (
          (LEN({{ column_name }} ) BETWEEN 3 AND 8 AND PATINDEX('[A-Z][0-9A-Z][0-9A-Z]%', {{ column_name }} ) = 1 AND {{ column_name }} NOT LIKE '%[^0-9A-Z]%') 
          OR
          (
            PATINDEX('%[0-9][.]%', {{ column_name }} ) = 1 AND (SUBSTRING({{ column_name }}, 0, CHARINDEX('.', {{ column_name }} )) LIKE '[A-Z][0-9]'
            OR SUBSTRING({{ column_name }} , 0, CHARINDEX('.', {{ column_name }} )) LIKE '[A-Z][0-9][0-9]') AND SUBSTRING({{ column_name }} , CHARINDEX('.', {{ column_name }} )+1,LEN({{ column_name }} )) NOT LIKE '%[^0-9A-Z]%'
          ) 
          OR 
          (
            PATINDEX('[0-9][0-9][0-9]%', {{ column_name }} ) = 1 AND (({{ column_name }}  NOT LIKE '%.%' AND LEN({{ column_name }} ) = 3) OR 
          {{ column_name }}  LIKE '%.%' AND (SUBSTRING({{ column_name }} , CHARINDEX('.', {{ column_name }} )+1,LEN({{ column_name }} )) like '[0-9]' OR SUBSTRING({{ column_name }} , CHARINDEX('.', {{ column_name }} )+1,LEN({{ column_name }} )) like '[0-9][0-9]')
            )
          )
          OR
          (
            PATINDEX('[0-9][0-9][0-9]%', {{ column_name }} ) = 1 AND {{ column_name }}  NOT LIKE '%[^0-9]%' AND LEN({{ column_name }} ) BETWEEN 3 AND 5
          )
          OR
          (
            PATINDEX('[VE][0-9][0-9][0-9]%', {{ column_name }} ) = 1 AND (({{ column_name }}  NOT LIKE '%.%' AND LEN({{ column_name }} ) = 4) 
            OR ({{ column_name }}  LIKE '%.%' AND SUBSTRING({{ column_name }} , CHARINDEX('.', {{ column_name }} )+1,LEN({{ column_name }} )) like '[0-9]')
          )
          )
          OR
          (
            PATINDEX('[VE][0-9][0-9]%', {{ column_name }} ) = 1 AND (SUBSTRING({{ column_name }} , CHARINDEX('V', {{ column_name }} )+1,LEN({{ column_name }} )) not like '%[^0-9]%' OR SUBSTRING({{ column_name }} , CHARINDEX('E', {{ column_name }} )+1,LEN({{ column_name }} )) not like '%[^0-9]%')
            AND PATINDEX('[VE][0-9][0-9]%', {{ column_name }} ) = 1 AND len({{ column_name }} ) between 3 and 5
          )
        )
      {% endif %}
    {% endfor %}
  {% else %}
    {{ dbt_expectations.test_expect_column_values_to_match_regex_list(model, column_name, regex_list, match_on, row_condition, is_raw, flags) }}
  {% endif %}
{% endtest %}    
