{#
    Required variable input: relation and column_list
    claim_type defaults to false if not provided

#}


{% macro medical_claim_date_check(relation, column_list, claim_type=false) %}
    {%- for column_item in column_list -%}
    select
          claim_id
        , data_source
        , '{{ column_item }}' as column_checked
    from {{ relation }} as rel
         left join {{ ref('terminology__calendar') }} as cal
           on rel.{{ column_item }} = cal.full_date
    where cal.full_date is null
    and rel.{{ column_item }} is not null
    {% if claim_type -%}
    and rel.claim_type = '{{ claim_type }}'
    {% endif -%}
    {% if not loop.last -%}
    union all
    {% endif -%}
    {%- endfor -%}
{% endmacro %}