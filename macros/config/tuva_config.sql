
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
