-- macros/log_warning_for_seeds.sql
{% macro log_warning_for_seeds() %}

    {%- set dbt_command = flags.WHICH -%}
    {% if dbt_command not in ['seed', 'build', 'run'] %}
        {% do return('') %}
    {% endif %}

    {% set seeds = get_selected_seeds() %}

    {% if seeds | length == 0 %}
        {% do return('') %}
    {% endif %}

    {# Batch all seed row counts into a single query instead of one query per seed.
       This avoids N serial round-trips (each with per-query warehouse overhead) at
       on-run-end. Pure UNION ALL / COUNT keeps it cross-database compatible. #}
    {% set selects = [] %}
    {% for seed in seeds %}
        {% do selects.append(
            "select " ~ dbt.string_literal(seed) ~ " as seed_name, count(*) as row_count from " ~ seed
        ) %}
    {% endfor %}
    {% set batched_query = selects | join('\nunion all\n') %}

    {% set results = run_query(batched_query) %}

    {% for row in results %}
        {% set seed_name = row[0] %}
        {% set row_count = row[1] %}
        {% if row_count == 0 %}
            {% if var('error_empty_seeds',False) == true %}
                {% do exceptions.raise_compiler_error ("The seed " ~ seed_name ~ " contains no data.  Check tuva:dbt_project.yml configurations to ensure data was correctly loaded with post hook") %}
            {% else %}
                {% do exceptions.warn("The seed " ~ seed_name ~ " contains no data.  Check tuva:dbt_project.yml configurations to ensure data was correctly loaded with post hook") %}
            {% endif %}
        {% endif %}
    {% endfor %}

{% endmacro %}
