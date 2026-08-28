{#

    Removes leading and trailing whitespace from a string expression.

    Every dialect Tuva targets today spells this the same way. The wrapper
    exists so a future adapter that does not can be handled in one place
    instead of at every call site.

#}

{% macro trim(expression) %}
  {{ return(adapter.dispatch('trim', 'the_tuva_project')(expression)) }}
{% endmacro %}

{% macro default__trim(expression) %}
  trim( {{ expression }} )
{% endmacro %}
