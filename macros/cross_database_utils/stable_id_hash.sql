{#
    Builds a portable deterministic identifier from an ordered list of fields.

    Each component is encoded independently before the components are joined:

      * null is encoded as N
      * a non-null value is prefixed with V
      * percent signs are escaped as %25
      * pipe characters are escaped as %7C

    Escaping percent before pipe makes the serialization unambiguous. For
    example, a literal %7C remains distinct from a literal pipe, null remains
    distinct from an empty string, and component boundaries cannot move when a
    source value contains the delimiter.

    Callers should include an explicit domain marker as the first field so
    that identical natural-key values in different entity domains cannot
    produce the same identifier.
#}

{% macro stable_id_input_string(fields) -%}
    {%- set parts = [] -%}
    {%- for field in fields -%}
        {%- set escaped_value =
            "replace(replace(cast(" ~ field ~ " as " ~ dbt.type_string() ~ "), '%', '%25'), '|', '%7C')"
        -%}
        {%- set encoded_value =
            "case when " ~ field ~ " is null then 'N' else "
            ~ the_tuva_project.concat_custom(["'V'", escaped_value]) ~ " end"
        -%}
        {%- do parts.append("(" ~ encoded_value ~ ")") -%}
        {%- if not loop.last -%}
            {%- do parts.append("'|'") -%}
        {%- endif -%}
    {%- endfor -%}
    {{ the_tuva_project.concat_custom(parts) }}
{%- endmacro %}


{% macro stable_id_hash(fields) -%}
    {{ return(adapter.dispatch('stable_id_hash', 'the_tuva_project')(fields)) }}
{%- endmacro %}


{% macro default__stable_id_hash(fields) -%}
    lower(md5({{ the_tuva_project.stable_id_input_string(fields) }}))
{%- endmacro %}


{# BigQuery's md5() returns bytes. #}
{% macro bigquery__stable_id_hash(fields) -%}
    lower(to_hex(md5({{ the_tuva_project.stable_id_input_string(fields) }})))
{%- endmacro %}


{# Athena/Trino md5() takes and returns varbinary. #}
{% macro athena__stable_id_hash(fields) -%}
    lower(to_hex(md5(to_utf8({{ the_tuva_project.stable_id_input_string(fields) }}))))
{%- endmacro %}


{# T-SQL has no md5(); hashbytes returns binary and defaults to uppercase hex. #}
{% macro fabric__stable_id_hash(fields) -%}
    lower(convert(varchar(32), hashbytes('MD5', {{ the_tuva_project.stable_id_input_string(fields) }}), 2))
{%- endmacro %}


{% macro sqlserver__stable_id_hash(fields) -%}
    lower(convert(varchar(32), hashbytes('MD5', {{ the_tuva_project.stable_id_input_string(fields) }}), 2))
{%- endmacro %}
