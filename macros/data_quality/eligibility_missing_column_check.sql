{#
    Required variable input: relation and column_list


#}


{% macro eligibility_missing_column_check(relation, column_list) %}
    {%- for column_item in column_list  %}
        select
              patient_id
            , data_source
            , '{{ column_item }}' as column_checked
        from {{ relation }}
        where {{ column_item }} is null
        {% if not loop.last -%}
            union all
        {%- endif -%}
        {%- endfor -%}
{% endmacro %}