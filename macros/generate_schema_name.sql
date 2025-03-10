{#
    This macro has been modified to work with the variables set in the dbt_project.yml file.
    See https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas for the original macro.
#}

{% macro default__generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {%- if default.startswith('_') and target.type == 'athena' -%}
            {{ default_schema[1:] | trim }}
        {%- else -%}
            {{ default_schema }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
