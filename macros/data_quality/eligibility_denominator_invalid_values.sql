{#
    Required variable input: relation and column_list

#}

{% macro eligibility_denominator_invalid_values(relation, column_list) %}
    {%- for column_item in column_list -%}
    select
          cat.test_name
        , count(distinct rel.patient_id) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ relation }} as rel
         left join {{ ref('data_quality__test_catalog') }} as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'eligibility'
           and cat.test_field = '{{ column_item }}'
    where rel.{{ column_item }} is not null
    group by cat.test_name
    {% if not loop.last -%}
    union all
    {% endif -%}
    {%- endfor -%}
{% endmacro %}