{%- macro apply_regex(column_name, regex) -%}

    {{ return(adapter.dispatch('apply_regex', 'the_tuva_project')(column_name, regex)) }}

{%- endmacro -%}

{%- macro default__apply_regex(column_name, regex) -%}

    regexp_like({{ column_name }}, '{{ regex}}')

{%- endmacro -%}

{%- macro snowflake__apply_regex(column_name, regex) -%}

    regexp_like({{ column_name }}, '{{ regex }}')

{%- endmacro -%}

{%- macro bigquery__apply_regex(column_name, regex) -%}

    regexp_contains({{ column_name }}, r'{{ regex }}')

{%- endmacro -%}

{%- macro postgres__apply_regex(column_name, regex) -%}

    {{ column_name }} similar to '{{ regex }}'

{%- endmacro -%}

{%- macro redshift__apply_regex(column_name, regex) -%}

     {{ column_name }} similar to '{{ regex }}'

{%- endmacro -%}

{%- macro duckdb__apply_regex(column_name, regex) -%}

    regexp_matches({{ column_name }}, '{{ regex }}')

{%- endmacro -%}

{%- macro clickhouse__apply_regex(column_name, regex) -%}

    match({{ column_name }}, '{{ regex }}')

{%- endmacro -%}
