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

{% macro is_tuva_selected() %}
  {# Check if any resources from the_tuva_project or tuva packages are selected #}
  {% if execute %}
    {% set nodes = graph.nodes.values() %}
    {% set selected_nodes = selected_resources %}

    {% for node in nodes %}
      {% if node.package_name in ['the_tuva_project', 'tuva'] and node.unique_id in selected_nodes %}
        {% do return(true) %}
      {% endif %}
    {% endfor %}

    {% do return(false) %}
  {% else %}
    {% do return(true) %}
  {% endif %}
{% endmacro %}
