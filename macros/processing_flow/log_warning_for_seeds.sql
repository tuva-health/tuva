-- macros/log_warning_for_seeds.sql
{% macro log_warning_for_seeds() %}
    {% set seeds = get_selected_seeds() %}

    {% for seed in seeds %}
        {% set sql_query = "select count(*) as row_count from " ~ seed %}
        {% set result = run_query(sql_query) %}
        {% set row_count = result[0][0] %}
        {% if row_count == 0 %}
            {% if var('error_empty_seeds',False) == true %}
                {% do exceptions.raise_compiler_error ("The seed " ~ seed ~ " contains no data.  Check tuva:dbt_project.yml configurations to ensure data was correctly loaded with post hook") %}
            {% else %}
                {% do exceptions.warn("The seed " ~ seed ~ " contains no data.  Check tuva:dbt_project.yml configurations to ensure data was correctly loaded with post hook") %}
            {% endif %}
        {% endif %}
    {% endfor %}

{% endmacro %}
