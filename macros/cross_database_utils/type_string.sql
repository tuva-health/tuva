{# Override string type for adapters with limited NVARCHAR support (e.g., Fabric) #}

{% macro fabric__type_string() -%}
    varchar(4000)
{%- endmacro %}

{% macro clickhouse__type_string() -%}
    Nullable(String)
{%- endmacro %}

{% macro clickhouse__type_int() -%}
    Nullable(Int32)
{%- endmacro %}

{% macro clickhouse__type_float() -%}
    Nullable(Float64)
{%- endmacro %}

{% macro clickhouse__type_numeric() -%}
    Nullable(Float64)
{%- endmacro %}

{% macro clickhouse__type_bigint() -%}
    Nullable(Int64)
{%- endmacro %}

{% macro clickhouse__type_timestamp() -%}
    Nullable(DateTime)
{%- endmacro %}
