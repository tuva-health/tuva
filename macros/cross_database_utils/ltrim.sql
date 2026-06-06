{#

    This macros performs a left trim of a string based on another string. Some databases order these 
    arguments differently!

#}

{%- macro ltrim(column_name, trimstr) -%}

    {{ return(adapter.dispatch('ltrim')(column_name, trimstr)) }}

{%- endmacro -%}

{%- macro default__ltrim(column_name, trimstr) -%}

    ltrim({{ column_name }}, '{{ trimstr }}')

{%- endmacro -%}


{%- macro databricks__ltrim(column_name, trimstr) -%}

    ltrim('{{ trimstr }}', {{ column_name }})

{%- endmacro -%}


{#- Trino/Athena has no two-argument ltrim(string, chars); use regexp_replace to
    strip the leading character set instead. -#}
{%- macro athena__ltrim(column_name, trimstr) -%}

    regexp_replace({{ column_name }}, '^[{{ trimstr }}]+', '')

{%- endmacro -%}
