{% macro get_installed_package_version(package_name) %}
  {% set conf = the_tuva_project.get_runtime_config() %}
  {% set normalized_package_name = package_name | string | trim %}

  {% if normalized_package_name == '' or normalized_package_name not in conf.dependencies %}
    {% do exceptions.raise_compiler_error(
        "Cannot resolve installed dbt package version for '" ~ package_name ~ "'."
    ) %}
  {% endif %}

  {% do return(conf.dependencies[normalized_package_name].version) %}
{% endmacro %}


{# Backwards-compatible Tuva Core package-version helper. #}
{% macro get_tuva_package_version() %}
  {% do return(the_tuva_project.get_installed_package_version('the_tuva_project')) %}
{% endmacro %}
