-- macros/log_warning_for_seeds.sql
{% macro log_warning_for_seeds() %}


    {# % do log("got here", info=True) % #}

    {% set seeds = get_selected_seeds() %}

    {# % do log("seeds: " ~ seeds, info=True) % #}
    {% for seed in seeds %}
        {# % do log("Looking at seed " ~ seed, info=True) % #}
        {% set sql_query = "select count(*) as row_count from " ~ seed %}
        {% set result = run_query(sql_query) %}
        {# % do log(sql_query, info=True) % #}
        {# % do log(result[0][0], info=True) % #}
        {% set row_count = result[0][0] %}
        {# % do log(flags,info=True) % #}
        {% if row_count == 0 %}
            {% if var('error_empty_seeds',False) == true %}
                {% do exceptions.raise_compiler_error ("The seed " ~ seed ~ " contains no data.  Check tuva:dbt_project.yml configurations to ensure data was correctly loaded with post hook") %}
            {% else %}
                {% do log("The seed " ~ seed ~ " contains no data.  Check tuva:dbt_project.yml configurations to ensure data was correctly loaded with post hook", info=True) %}
            {% endif %}
        {% endif %}
    {% endfor %}

{% endmacro %}
