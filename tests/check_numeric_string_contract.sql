{{ config(
     severity = 'error',
     tags = ['contract', 'numeric_string_contract']
   )
}}

{#
  Numeric strings are a cross-warehouse lexical contract. The fixture includes
  signed integers and decimals, a leading decimal point, malformed punctuation,
  non-decimal notation, whitespace, and null behavior.
#}

{% set numeric_string_cases = [
    ('zero', "'0'", '1'),
    ('unsigned_integer', "'123'", '1'),
    ('positive_integer', "'+123'", '1'),
    ('negative_integer', "'-123'", '1'),
    ('leading_decimal_point', "'.5'", '1'),
    ('signed_leading_decimal_point', "'-.5'", '1'),
    ('unsigned_decimal', "'0.5'", '1'),
    ('positive_decimal', "'+0.5'", '1'),
    ('negative_decimal', "'-0.5'", '1'),
    ('leading_zeroes', "'0001.20'", '1'),
    ('empty_string', "''", '0'),
    ('sign_only', "'+'", '0'),
    ('decimal_point_only', "'.'", '0'),
    ('signed_decimal_point_only', "'-.'", '0'),
    ('trailing_decimal_point', "'1.'", '0'),
    ('multiple_decimal_points', "'1..2'", '0'),
    ('alphabetic_prefix', "'a1'", '0'),
    ('alphabetic_suffix', "'1a'", '0'),
    ('leading_whitespace', "' 1'", '0'),
    ('trailing_whitespace', "'1 '", '0'),
    ('exponent_notation', "'1e3'", '0'),
    ('multiple_signs', "'--1'", '0'),
    ('embedded_sign', "'1-2'", '0'),
    ('thousands_separator', "'1,000'", '0'),
    ('nan_literal', "'NaN'", '0'),
    (
        'null_input',
        'cast(null as ' ~ dbt.type_string() ~ ')',
        'cast(null as ' ~ dbt.type_int() ~ ')'
    )
] %}

with test_cases as (
    {% for case in numeric_string_cases %}
    select
          '{{ case[0] }}' as case_name
        , {{ case[1] }} as input_value
        , {{ case[2] }} as expected_result
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

, actual_results as (
    select
          case_name
        , cast(case
            when {{ the_tuva_project.is_numeric_string('input_value') }} then 1
            when not ({{ the_tuva_project.is_numeric_string('input_value') }}) then 0
          end as {{ dbt.type_int() }}) as actual_result
        , expected_result
    from test_cases
)

select
      case_name
    , actual_result
    , expected_result
from actual_results
where coalesce(actual_result, -1) <> coalesce(expected_result, -1)
