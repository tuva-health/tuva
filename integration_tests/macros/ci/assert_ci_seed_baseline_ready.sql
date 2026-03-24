{% macro assert_ci_seed_baseline_ready() %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {# 
      Run-only CI modes depend on these preloaded schemas.
      We validate one canonical relation per schema to keep this precheck fast.
    #}
    {% set required_relations = [
        {'schema': 'raw_data', 'identifier': 'eligibility'},
        {'schema': 'provider-data', 'identifier': 'provider'},
        {'schema': 'terminology', 'identifier': 'admit_type'},
        {'schema': 'reference_data', 'identifier': 'calendar'},
        {'schema': 'concept_library', 'identifier': 'clinical_concepts'}
    ] %}

    {% set db_name = target.database if target.database is not none else none %}
    {% set missing = [] %}

    {% for required in required_relations %}
        {% set relation = adapter.get_relation(
            database=db_name,
            schema=required['schema'],
            identifier=required['identifier']
        ) %}
        {% if relation is none %}
            {% do missing.append(required['schema'] ~ '.' ~ required['identifier']) %}
        {% endif %}
    {% endfor %}

    {% if missing | length > 0 %}
        {% do exceptions.raise_compiler_error(
            "CI baseline seed schemas are not ready for run-only mode. Missing required objects: "
            ~ (missing | join(', '))
            ~ ". Run `/ci large` on this PR to refresh `raw_data`, `provider-data`, `terminology`, `reference_data`, and `concept_library`."
        ) %}
    {% endif %}

{% endmacro %}
