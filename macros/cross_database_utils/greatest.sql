{#

    Returns the larger of two expressions.

    This mirrors `least` exactly, including its null handling: warehouses
    disagree about whether a null argument makes the whole call null
    (Snowflake, BigQuery and Redshift propagate it; DuckDB, Postgres and
    Databricks ignore it), so both macros express the comparison with a
    case expression and treat null as "no value" rather than "unknown".

#}

{% macro greatest(a, b) %}
  {{ return(adapter.dispatch('greatest', 'the_tuva_project')(a, b)) }}
{% endmacro %}

{% macro default__greatest(a, b) %}
  case
    when {{ a }} is null and {{ b }} is null then null
    when {{ a }} is null then {{ b }}
    when {{ b }} is null then {{ a }}
    when {{ a }} >= {{ b }} then {{ a }}
    else {{ b }}
  end
{% endmacro %}
