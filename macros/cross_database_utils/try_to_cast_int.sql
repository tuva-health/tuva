{#

    This macro takes in a column and attempts to cast it to integer,
    returning NULL on failure (adapter-specific).

#}

{%- macro try_to_cast_int(column_name) -%}

    {{ return(adapter.dispatch('try_to_cast_int')(column_name)) }}

{%- endmacro -%}

{%- macro bigquery__try_to_cast_int(column_name) -%}

    safe_cast( {{ column_name }} as int64 )

{%- endmacro -%}

{%- macro default__try_to_cast_int(column_name) -%}

    try_cast( {{ column_name }} as integer )

{%- endmacro -%}

{%- macro postgres__try_to_cast_int(column_name) -%}

    {{ dbt.safe_cast(column_name, api.Column.translate_type("integer")).strip() }}

{%- endmacro -%}

{%- macro redshift__try_to_cast_int(column_name) -%}

    try_cast( {{ column_name }} as integer )

{%- endmacro -%}

{%- macro snowflake__try_to_cast_int(column_name) -%}

    try_cast( {{ column_name }} as integer )

{%- endmacro -%}

{%- macro athena__try_to_cast_int(column_name) -%}

    try_cast( {{ column_name }} as integer )

{%- endmacro -%}
