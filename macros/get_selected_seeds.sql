-- macros/get_selected_seeds.sql
{% macro get_selected_seeds() %}
{% if execute %}
    {% set nodes = graph.nodes.values() %}
    {% set selected_nodes = selected_resources %}
    {% set ns = namespace(selected_seeds=[]) %}

    {% for node in nodes %}
        {% if node.resource_type == 'seed' and node.unique_id in selected_nodes and node.package_name in['the_tuva_project','tuva','integration_tests'] %}
            {% if adapter.config.quoting.database %}
                {% set db =adapter.quote(node.database) %}
            {% else %}
                {% set db = node.database %}
            {% endif %}
            {% if adapter.config.quoting.schema %}
                {% set sch =adapter.quote(node.schema) %}
            {% else %}
                {% set sch = node.schema %}
            {% endif %}
            {% if adapter.config.quoting.identifier %}
                {% set idf =adapter.quote(node.alias) %}
            {% else %}
                {% set idf = node.alias %}
            {% endif %}
            {% set fully_qualified_name = db ~ '.' ~ sch ~ '.' ~ idf %}

            {% do ns.selected_seeds.append(fully_qualified_name) %}
        {% endif %}
    {% endfor %}

    {{ return(ns.selected_seeds) }}
{% else %}
    {{ return([]) }}
{% endif %}
{% endmacro %}
