{% macro get_default_config() %}
  {# Centralized default configuration for all Tuva hooks #}
  {% set default_config = {
    'disable_tuva_invocation_tracking': false
  } %}
  {% do return(default_config) %}
{% endmacro %}

{% macro get_config_var(var_name) %}
  {# Read dbt variable with fallback to defaults #}
  {% set default_config = get_default_config() %}
  {% set var_value = var(var_name, default_config.get(var_name)) %}

  {# Handle string-to-boolean conversion #}
  {% if var_value is string %}
    {% if var_value.lower() == "true" %}
      {% do return(true) %}
    {% elif var_value.lower() == "false" %}
      {% do return(false) %}
    {% endif %}
  {% endif %}

  {% do return(var_value) %}
{% endmacro %}
