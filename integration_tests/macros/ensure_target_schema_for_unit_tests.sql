{#
    dbt unit tests can use target.schema for temporary relations. Because every
    integration-test node has a custom schema, dbt does not otherwise create it.
#}
{% macro ensure_target_schema_for_unit_tests() -%}
    {%- if execute -%}
        {%- set target_schema_relation = api.Relation.create(
            database=target.database,
            schema=target.schema
        ) -%}
        {%- do adapter.create_schema(target_schema_relation) -%}
    {%- endif -%}
{%- endmacro %}
