{#
    Required variable input: relation and column_list


#}


{% macro eligibility_missing_column_check(relation, column_list) %}


    {%- set all_columns = adapter.get_columns_in_relation(
        relation
    ) -%}

    {%- for column_item in all_columns
        if column_item.name.lower() in column_list %}

        select
            patient_id
            , '{{ column_item.name|lower }}' as column_checked
        from {{ relation }}
        where {{ column_item.name }} is null



        {% if not loop.last -%}
            union all
        {%- endif -%}

        {%- endfor -%}

{% endmacro %}