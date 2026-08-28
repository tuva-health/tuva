{# dbt has no supported Jinja API for package metadata; contract tests keep this literal aligned with dbt_project.yml. #}
{% macro get_tuva_package_version() %}
  {% do return('1.0.0') %}
{% endmacro %}
