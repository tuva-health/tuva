{#

    Divides `numerator` by `denominator`, returning null instead of raising
    when the denominator is zero.

#}

{% macro safe_divide(numerator, denominator) %}
  {{ return(adapter.dispatch('safe_divide', 'the_tuva_project')(numerator, denominator)) }}
{% endmacro %}

{% macro default__safe_divide(numerator, denominator) %}
  ( {{ numerator }} ) / nullif( {{ denominator }}, 0 )
{% endmacro %}

{% macro bigquery__safe_divide(numerator, denominator) %}
  safe_divide( {{ numerator }}, {{ denominator }} )
{% endmacro %}
