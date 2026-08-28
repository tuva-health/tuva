{#

    Renders every macro in the cross-database function layer against literal
    inputs, so a single build proves on the target adapter that each macro
    compiles and returns its documented value. The companion schema file
    asserts those values.

    Two adapter-shaped decisions:

    - Fabric spells booleans `bit` and has no boolean literal, so the literal
      rows below pick their boolean type and literals from `target.type`, the
      same way the Core unit tests pick their timestamp type.
    - The boolean aggregates are cast to int in the output so one assertion
      covers both the boolean-returning adapters and Fabric's bit.

#}

{{ config(materialized='table') }}

{%- set boolean_type = 'bit' if target.type in ['fabric', 'sqlserver'] else 'boolean' -%}
{%- set true_literal = '1' if target.type in ['fabric', 'sqlserver'] else 'true' -%}
{%- set false_literal = '0' if target.type in ['fabric', 'sqlserver'] else 'false' -%}
{%- set decimal_type = 'numeric' if target.type == 'bigquery' else 'decimal(18, 6)' -%}

with literal_rows as (

    select
        1 as ordinal
      , cast({{ true_literal }} as {{ boolean_type }}) as flag
      , cast('a' as {{ dbt.type_string() }}) as token

    union all
    select
        2 as ordinal
      , cast({{ false_literal }} as {{ boolean_type }}) as flag
      , cast('b' as {{ dbt.type_string() }}) as token

    union all
    select
        3 as ordinal
      , cast({{ true_literal }} as {{ boolean_type }}) as flag
      , cast('c' as {{ dbt.type_string() }}) as token

)

, scalar_expectations as (

    select
        {{ the_tuva_project.trim("'  padded  '") }} as trim_result
      , {{ the_tuva_project.left("'abcdef'", 3) }} as left_result
      , {{ the_tuva_project.greatest(7, 3) }} as greatest_result
      , {{ the_tuva_project.greatest('cast(null as int)', 3) }} as greatest_null_result
      , {{ the_tuva_project.safe_divide('cast(10 as ' ~ decimal_type ~ ')', 'cast(4 as ' ~ decimal_type ~ ')') }} as safe_divide_result
      , {{ the_tuva_project.safe_divide('cast(10 as ' ~ decimal_type ~ ')', 'cast(0 as ' ~ decimal_type ~ ')') }} as safe_divide_by_zero_result
      , {{ the_tuva_project.round('cast(3.14159 as ' ~ decimal_type ~ ')', 2) }} as round_result
      , {{ the_tuva_project.round('cast(3.6 as ' ~ decimal_type ~ ')') }} as round_default_precision_result

)

, aggregate_expectations as (

    select
        cast({{ the_tuva_project.bool_and_agg('flag') }} as int) as bool_and_agg_result
      , cast({{ the_tuva_project.bool_or_agg('flag') }} as int) as bool_or_agg_result
      , {{ the_tuva_project.string_agg('token', "','", 'order by ordinal') }} as string_agg_result
    from literal_rows

)

select *
from scalar_expectations
cross join aggregate_expectations
