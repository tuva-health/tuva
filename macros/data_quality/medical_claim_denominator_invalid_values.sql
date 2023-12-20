{#
    Required variable input: relation

    The first step of this macro is to query the test catalog to retrieve
    the test_field for all invalid_value tests for medical_claim.

    The second step sets the results of the query to a list.

    The last step is to loop through the list and generate the SQL statements
    for the CTE that builds the denominators for invalid value tests.
#}

{% macro medical_claim_denominator_invalid_values(relation) %}
{%- set sql_statement -%}
    select test_field
    from {{ ref('data_quality__test_catalog') }}
    where source_table = 'normalized_input__medical_claim'
    and test_category = 'invalid_values'
{%- endset -%}

{%- set results = run_query(sql_statement) -%}

{%- if execute -%}
{%- set results_list = results.columns[0].values() -%}
{%- else -%}
{%- set results_list = [] -%}
{%- endif -%}

    {%- for test_field in results_list -%}
    select
          cat.test_name
        , count(distinct rel.claim_id||rel.data_source) as denominator
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ relation }} as rel
         left join {{ ref('data_quality__test_catalog') }} as cat
           on cat.test_category = 'invalid_values'
           and cat.source_table = 'normalized_input__medical_claim'
           and cat.test_field = '{{ test_field }}'
    where rel.{{ test_field }} is not null
    group by cat.test_name
    {% if not loop.last -%}
    union all
    {% endif -%}
    {%- endfor -%}
{% endmacro %}