{% macro header_validation_diagnosis_code(relation, column_list, claim_type) %}
    {%- for column_item in column_list %}
        select
            claim_id
            , '{{ column_item }}' as column_checked
            , count(distinct {{ column_item }}) as duplicate_count
        from {{ relation }}
        where claim_type = '{{ claim_type }}'
        group by claim_id
        having count(distinct {{ column_item }}) > 1
        {% if not loop.last -%}
            union all
        {%- endif -%}
        {%- endfor -%}
{% endmacro %}