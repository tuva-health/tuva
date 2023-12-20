{#
    Required variable input: relation, column_list, and claim_type

#}


{% macro medical_claim_header_duplicate_check(relation, column_list, claim_type) %}
    {%- for column_item in column_list %}
        select
              claim_id
            , data_source
            , '{{ column_item }}' as column_checked
            , count(distinct {{ column_item }}) as duplicate_count
        from {{ relation }}
        where claim_type = '{{ claim_type }}'
        group by
              claim_id
            , data_source
        having count(distinct {{ column_item }}) > 1
        {% if not loop.last -%}
            union all
        {%- endif -%}
        {%- endfor -%}
{% endmacro %}