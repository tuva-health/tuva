{% macro load_provider_data_seed(pattern) %}
  {{ return(adapter.dispatch('load_provider_data_seed', 'the_tuva_project')(pattern)) }}
{% endmacro %}

{% macro default__load_provider_data_seed(pattern) %}
  {{ return(the_tuva_project.load_versioned_seed('provider_data', pattern)) }}
{% endmacro %}
