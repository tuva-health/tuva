{% macro header_validation_diagnosis_voting(table_1, table_2, column_list) %}
    {%- for column_item in column_list %}
        select 
            norm.claim_id
            , norm.data_source
            , '{{ column_item }}' as column_checked
            , norm.{{ column_item }} as diagnosis_normalized
            , count(*) as occurrence_count
            , coalesce(lead(count(*)) over(partition by norm.claim_id order by count(*) desc),0) as next_occurrence_count
            , row_number() over (partition by norm.claim_id order by count(*) desc) as occurrence_row_count
        from {{ table_1 }} norm
        inner join {{ table_2 }} uni
            on norm.claim_id = uni.claim_id
            and norm.data_source = uni.data_source
            and uni.column_checked = '{{ column_item }}'
        group by 
            norm.claim_id
            , norm.data_source
            , norm.{{ column_item }}
        {% if not loop.last -%}
            union all
        {%- endif -%}
        {%- endfor -%}
{% endmacro %}