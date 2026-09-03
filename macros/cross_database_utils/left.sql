{#

    Returns the leftmost `length` characters of a string expression.

    Every dialect Tuva targets today spells this the same way. The wrapper
    exists so a future adapter that does not can be handled in one place
    instead of at every call site.

#}

{% macro left(expression, length) %}
  {{ return(adapter.dispatch('left', 'the_tuva_project')(expression, length)) }}
{% endmacro %}

{% macro default__left(expression, length) %}
  left( {{ expression }}, {{ length }} )
{% endmacro %}


{# Athena/Trino has no left(); substr is the portable spelling. #}
{% macro athena__left(expression, length) %}
  substr( {{ expression }}, 1, {{ length }} )
{% endmacro %}
