{% macro length(col) %}
  {{ return(adapter.dispatch('length', 'the_tuva_project')(col)) }}
{% endmacro %}

{% macro default__length(col) %}
  length( {{ col }} )
{% endmacro %}

{% macro fabric__length(col) %}
  len( {{ col }} )
{% endmacro %}
