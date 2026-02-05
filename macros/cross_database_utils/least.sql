{% macro least(a, b) %}
  {{ return(adapter.dispatch('least', 'macros')(a, b)) }}
{% endmacro %}

{% macro default__least(a, b) %}
  case
    when {{ a }} is null and {{ b }} is null then null
    when {{ a }} is null then {{ b }}
    when {{ b }} is null then {{ a }}
    when {{ a }} <= {{ b }} then {{ a }}
    else {{ b }}
  end
{% endmacro %}
