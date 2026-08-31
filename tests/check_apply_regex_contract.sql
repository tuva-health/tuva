{{ config(
     enabled = target.type != 'fabric',
     severity = 'error',
     tags = ['contract', 'regex_contract']
   )
}}

{#
  apply_regex is a case-sensitive search predicate. These fixtures distinguish
  search semantics from an implicit full-string match and pin explicit anchor
  and null behavior across the adapters that expose regular expressions.
#}

{% if target.type != 'fabric' %}
{% set regex_cases = [
    ('unanchored_search', "'prefix123suffix'", '[0-9]+', '1'),
    ('explicit_start_anchor_rejects_prefix', "'prefix123suffix'", '^[0-9]+', '0'),
    ('explicit_end_anchor_rejects_suffix', "'prefix123suffix'", '[0-9]+$', '0'),
    ('explicit_full_string_match', "'123'", '^[0-9]+$', '1'),
    ('case_sensitive_rejection', "'ABC'", '^[a-z]+$', '0'),
    ('case_sensitive_match', "'abc'", '^[a-z]+$', '1'),
    (
        'null_input',
        'cast(null as ' ~ dbt.type_string() ~ ')',
        '[0-9]+',
        'cast(null as ' ~ dbt.type_int() ~ ')'
    )
] %}

with contract_cases as (
    {% for case in regex_cases %}
    select
          '{{ case[0] }}' as case_name
        , cast(case
            when {{ the_tuva_project.apply_regex(case[1], case[2]) }} then 1
            when not ({{ the_tuva_project.apply_regex(case[1], case[2]) }}) then 0
          end as {{ dbt.type_int() }}) as actual_result
        , {{ case[3] }} as expected_result
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select
      case_name
    , actual_result
    , expected_result
from contract_cases
where coalesce(actual_result, -1) <> coalesce(expected_result, -1)
{% else %}
select 1 as disabled_on_fabric
from (select 1 as _tuva_empty_fixture) as _tuva_empty_fixture
where 1 = 0
{% endif %}
