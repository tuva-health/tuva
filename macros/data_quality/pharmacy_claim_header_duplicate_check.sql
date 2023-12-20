{#
    Required variable input: relation and column_list

#}


{% macro pharmacy_claim_header_duplicate_check(relation, column_list) %}
    {%- for column_item in column_list %}
        select
              claim_id
            , data_source
            , '{{ column_item }}' as column_checked
            , count(distinct {{ column_item }}) as duplicate_count
        from {{ relation }}
        group by claim_id, data_source
        having count(distinct {{ column_item }}) > 1
        {% if not loop.last -%}
            union all
        {%- endif -%}
        {%- endfor -%}
{% endmacro %}