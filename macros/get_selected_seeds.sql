-- macros/get_selected_seeds.sql
{% macro get_selected_seeds() %}
{% if execute %}
    {% set nodes = graph.nodes.values() %}
    {% set selected_nodes = selected_resources %}
    {% set ns = namespace(selected_seeds=[]) %}

    {% for node in nodes %}
        {% if node.resource_type == 'seed' and node.unique_id in selected_nodes and node.package_name in['the_tuva_project','tuva'] %}
            {% set fully_qualified_name = node.database ~ '.' ~ node.schema ~ '.' ~ node.alias %}
            {% do ns.selected_seeds.append(fully_qualified_name) %}
        {% endif %}
    {% endfor %}

    {{ return(ns.selected_seeds) }}
{% else %}
    {{ return([]) }}
{% endif %}
{% endmacro %}
