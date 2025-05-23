{# This will get the version variable defined in the tuva project dbt_project.yml file #}
{% macro get_tuva_package_version() %}
  {% set conf = the_tuva_project.get_runtime_config() %}
  {% do return(conf.dependencies["the_tuva_project"].version) %}
{% endmacro %}
