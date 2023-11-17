{% macro header_validation_diagnosis_unique_count(table1, column_list) %}
    {%- for column_item in column_list %}
        select
            claim_id
            , data_source
            , '{{ column_item }}' as column_checked
            , count(distinct {{ column_item }}) as distinct_count
        from {{ table1 }}
        group by 
            claim_id
            , data_source
        {% if not loop.last -%}
            union all
        {%- endif -%}
        {%- endfor -%}
{% endmacro %}