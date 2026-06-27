{#
    This macro has been modified to work with the variables set in the dbt_project.yml file.
    See https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas for the original macro.
#}

{% macro default__generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set explicit_prefix = var('tuva_schema_prefix', none) -%}
    {%- set has_explicit_prefix = explicit_prefix is not none and explicit_prefix | trim != '' -%}
    {%- set ci_run_id = env_var('GITHUB_RUN_ID', '') | trim -%}
    {%- set ci_prefix = 'gh' ~ ci_run_id if not has_explicit_prefix and ci_run_id != '' else none -%}

    {%- if custom_schema_name is not none -%}
        {%- set custom_schema = custom_schema_name | trim -%}
        {%- if ci_prefix is not none -%}
            {%- set normalized_custom_schema = custom_schema[1:] if custom_schema.startswith('_') else custom_schema -%}
            {{ ci_prefix }}_{{ normalized_custom_schema }}
        {%- else -%}
            {{ custom_schema }}
        {%- endif -%}
    {%- else -%}
        {%- if ci_prefix is not none -%}
            {{ ci_prefix }}_{{ default_schema }}
        {%- else -%}
            {{ default_schema }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
