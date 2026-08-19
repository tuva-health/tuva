{#
    Builds a deterministic encounter_id from the natural key of the encounter.

    encounter_id used to be a dense_rank() over the whole table, which meant the
    value assigned to an encounter depended on every other encounter in the run:
    one new patient sorting early shifted every id after it, so ids changed on
    every refresh and could not be compared across runs, across partial runs, or
    against a downstream table built from an earlier run.

    Hashing the natural key instead makes the id a pure function of the row, so
    the same encounter keeps the same id no matter what else is in the data.

    Fields are cast to string, null-marked and pipe-delimited before hashing so
    that a null and an empty string cannot produce the same input, and neither
    can ('a','bc') and ('ab','c').
#}

{% macro encounter_id_input_string(fields) -%}
    {%- set parts = [] -%}
    {%- for field in fields -%}
        {%- do parts.append("coalesce(cast(" ~ field ~ " as " ~ dbt.type_string() ~ "), '<NULL>')") -%}
        {%- if not loop.last -%}{%- do parts.append("'|'") -%}{%- endif -%}
    {%- endfor -%}
    {{ the_tuva_project.concat_custom(parts) }}
{%- endmacro %}


{% macro encounter_id_hash(fields) -%}
    {{ return(adapter.dispatch('encounter_id_hash', 'the_tuva_project')(fields)) }}
{%- endmacro %}


{% macro default__encounter_id_hash(fields) -%}
    md5({{ the_tuva_project.encounter_id_input_string(fields) }})
{%- endmacro %}


{# BigQuery's md5() returns bytes. #}
{% macro bigquery__encounter_id_hash(fields) -%}
    to_hex(md5({{ the_tuva_project.encounter_id_input_string(fields) }}))
{%- endmacro %}


{# Athena/Trino md5() takes and returns varbinary. #}
{% macro athena__encounter_id_hash(fields) -%}
    lower(to_hex(md5(to_utf8({{ the_tuva_project.encounter_id_input_string(fields) }}))))
{%- endmacro %}


{# T-SQL has no md5(); hashbytes returns binary and defaults to uppercase hex. #}
{% macro fabric__encounter_id_hash(fields) -%}
    lower(convert(varchar(32), hashbytes('MD5', {{ the_tuva_project.encounter_id_input_string(fields) }}), 2))
{%- endmacro %}


{% macro sqlserver__encounter_id_hash(fields) -%}
    lower(convert(varchar(32), hashbytes('MD5', {{ the_tuva_project.encounter_id_input_string(fields) }}), 2))
{%- endmacro %}
