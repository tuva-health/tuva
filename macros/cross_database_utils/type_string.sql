{# Override string type for adapters with limited NVARCHAR support (e.g., Fabric) #}

{% macro fabric__type_string() -%}
    varchar(4000)
{%- endmacro %}
