{% macro calculate_age(birthdate_column, target_date_column=None) %}
  {% if target_date_column is none %}
    {% set target_date_column = 'current_date()' %}
  {% endif %}

  {% if target.type == 'snowflake' %}
    datediff('year', {{ birthdate_column }}, {{ target_date_column }}) -
    case
      when dateadd('year', datediff('year', {{ birthdate_column }}, {{ target_date_column }}), {{ birthdate_column }}) > {{ target_date_column }}
      then 1
      else 0
    end
  {% elif target.type == 'bigquery' %}
    date_diff({{ target_date_column }}, {{ birthdate_column }}, year) -
    case
      when date_add({{ birthdate_column }}, interval date_diff({{ target_date_column }}, {{ birthdate_column }}, year) year) > {{ target_date_column }}
      then 1
      else 0
    end
  {% elif target.type == 'postgres' or target.type == 'redshift' %}
    date_part('year', age({{ target_date_column }}, {{ birthdate_column }}))
  {% elif target.type == 'databricks' %}
    year({{ target_date_column }}) - year({{ birthdate_column }}) -
    case
      when date_add({{ birthdate_column }}, (year({{ target_date_column }}) - year({{ birthdate_column }})) * interval 1 year) > {{ target_date_column }}
      then 1
      else 0
    end
  {% else %}
    -- Default calculation for other databases - uses year difference approach
    year({{ target_date_column }}) - year({{ birthdate_column }}) -
    case
      when month({{ target_date_column }}) < month({{ birthdate_column }})
        or (month({{ target_date_column }}) = month({{ birthdate_column }}) and day({{ target_date_column }}) < day({{ birthdate_column }}))
      then 1
      else 0
    end
  {% endif %}
{% endmacro %}