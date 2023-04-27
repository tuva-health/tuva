{#
    Required variable input: relation and column_list

#}


{% macro header_duplicate_check(relation, column_list, claim_type) %}


    {%- set all_columns = adapter.get_columns_in_relation(
        relation
    ) -%}

    {%- for column_item in all_columns
        if column_item.name.lower() in column_list %}

        select
            claim_id
            , '{{ column_item.name|lower }}' as column_checked
            , count(distinct {{ column_item.name }}) as duplicate_count
        from {{ relation }}
        where claim_type = '{{ claim_type }}'
        group by claim_id
        having count(distinct {{ column_item.name }}) > 1

        {% if not loop.last -%}
            union all
        {%- endif -%}

        {%- endfor -%}

{% endmacro %}