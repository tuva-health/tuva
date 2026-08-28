{#

    Aggregate that is true when every non-null input row is true.

    Fabric has no boolean aggregate, so it takes the minimum of the bit
    column widened to int and narrows the result back to bit.

#}

{% macro bool_and_agg(expression) %}
  {{ return(adapter.dispatch('bool_and_agg', 'the_tuva_project')(expression)) }}
{% endmacro %}

{% macro default__bool_and_agg(expression) %}
  bool_and( {{ expression }} )
{% endmacro %}

{% macro snowflake__bool_and_agg(expression) %}
  booland_agg( {{ expression }} )
{% endmacro %}

{% macro bigquery__bool_and_agg(expression) %}
  logical_and( {{ expression }} )
{% endmacro %}

{% macro fabric__bool_and_agg(expression) %}
  cast( min( cast( {{ expression }} as int) ) as bit)
{% endmacro %}
