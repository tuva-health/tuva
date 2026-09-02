{#
    Returns whether a string has the lexical form of a decimal number:

      * an optional leading + or - sign
      * zero or more digits before an optional decimal point
      * one or more trailing digits

    This intentionally rejects whitespace, exponent notation, thousands
    separators, and a trailing decimal point. A null input returns null.
#}
{%- macro is_numeric_string(column_name) -%}

    {{ return(adapter.dispatch('is_numeric_string', 'the_tuva_project')(column_name)) }}

{%- endmacro -%}

{%- macro default__is_numeric_string(column_name) -%}

    {{ the_tuva_project.apply_regex(column_name, '^[+-]?([0-9]*[.])?[0-9]+$') }}

{%- endmacro -%}

{%- macro fabric__is_numeric_string(column_name) -%}

    {%- set unsigned_value -%}
        case
            when left({{ column_name }}, 1) in ('+', '-')
                then substring(
                    {{ column_name }},
                    2,
                    datalength({{ column_name }})
                )
            else {{ column_name }}
        end
    {%- endset -%}

    (
            {{ unsigned_value }} like '%[0123456789]'
        and {{ unsigned_value }} not like '%[^0123456789.]%'
        and {{ unsigned_value }} not like '%.%.%'
        and datalength({{ unsigned_value }})
            = datalength(rtrim({{ unsigned_value }}))
    )

{%- endmacro -%}

{# SQL Server is T-SQL. dbt-sqlserver registers no adapter-type parent,
   so fabric__ macros are not reached by dispatch and must be aliased. #}
{% macro sqlserver__is_numeric_string(column_name) -%}
    {{ the_tuva_project.fabric__is_numeric_string(column_name) }}
{%- endmacro %}
