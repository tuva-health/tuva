{#
    This macro has been modified to work with the variables set in the dbt_project.yml file.
    See https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas for the original macro.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {{ custom_schema_name }}

{%- endmacro %}