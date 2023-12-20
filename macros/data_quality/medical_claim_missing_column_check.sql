{#
    Required variable input: relation and column_list


#}


{% macro medical_claim_missing_column_check(relation, column_list, claim_type) %}
    {%- for column_item in column_list %}
        select
              claim_id
            , data_source
            , '{{ column_item }}' as column_checked
        from {{ relation }}
        where  {{ column_item }} is null
        and claim_type = '{{ claim_type }}'
        {% if not loop.last -%}
            union all
        {%- endif -%}
        {%- endfor -%}
{% endmacro %}
