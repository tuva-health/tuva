{% macro hcc_suspecting__observation_suspects_actual() %}

-- Default: if a final model exists, point to it. Override in the project by creating a higher-priority macro impl.
{% if execute %}
    {% set candidate_models = [
        'hcc_suspecting__observation_suspects',
        'observation_suspects',
        'hcc_suspecting__observation_suspects_basic'
    ] %}
    {% set found = none %}
    {% for m in candidate_models %}
        {% if graph.nodes.get('model.' ~ project_name ~ '.' ~ m) %}
            {% set found = m %}
            {% break %}
        {% endif %}
    {% endfor %}
{% endif %}

{% if found is not none %}
select * from {{ ref(found) }}
{% else %}
-- Fallback: inline the "basic" test SQL to derive ACTUAL consistently with existing test.
{{ return(adapter.dispatch('observation_suspects_basic_cte', 'hcc_suspecting')()) }}
{% endif %}
{% endmacro %}