{#
    Returns a case-sensitive regular-expression search predicate. Patterns are
    not implicitly anchored; callers can use ^ and $ when they require a
    full-string match. The default implementation covers Databricks.

    Fabric Warehouse does not expose a general-purpose regular-expression
    predicate. Use a narrower portable predicate such as is_numeric_string
    when the required semantics can be expressed across every adapter.
#}
{%- macro apply_regex(column_name, regex) -%}

    {{ return(adapter.dispatch('apply_regex', 'the_tuva_project')(column_name, regex)) }}

{%- endmacro -%}

{%- macro default__apply_regex(column_name, regex) -%}

    regexp_like({{ column_name }}, '{{ regex }}')

{%- endmacro -%}

{%- macro snowflake__apply_regex(column_name, regex) -%}

    regexp_instr({{ column_name }}, '{{ regex }}') > 0

{%- endmacro -%}

{%- macro bigquery__apply_regex(column_name, regex) -%}

    regexp_contains({{ column_name }}, r'{{ regex }}')

{%- endmacro -%}

{%- macro fabric__apply_regex(column_name, regex) -%}

    {{ exceptions.raise_compiler_error(
        "the_tuva_project.apply_regex is not supported on Fabric Warehouse. "
        ~ "Use a portable domain predicate such as "
        ~ "the_tuva_project.is_numeric_string instead."
    ) }}

{%- endmacro -%}

{%- macro postgres__apply_regex(column_name, regex) -%}

    {{ column_name }} ~ '{{ regex }}'

{%- endmacro -%}

{%- macro redshift__apply_regex(column_name, regex) -%}

    {{ column_name }} ~ '{{ regex }}'

{%- endmacro -%}

{%- macro duckdb__apply_regex(column_name, regex) -%}

    regexp_matches({{ column_name }}, '{{ regex }}')

{%- endmacro -%}

{# SQL Server is T-SQL. dbt-sqlserver registers no adapter-type parent,
   so fabric__ macros are not reached by dispatch and must be aliased. #}
{% macro sqlserver__apply_regex(column_name, regex) -%}
    {{ the_tuva_project.fabric__apply_regex(column_name, regex) }}
{%- endmacro %}
