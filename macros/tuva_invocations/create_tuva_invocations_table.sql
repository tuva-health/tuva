{% macro create_tuva_invocations_table() %}
    {% do adapter.create_schema(api.Relation.create(database=target.database, schema="metadata")) %}
    {% set create_table_query %}
        create table if not exists metadata.tuva_invocations
        (
            invocation_id {{ dbt.type_string() }},
            project_name {{ dbt.type_string() }},
            tuva_package_version {{ dbt.type_string() }},
            run_started_at {{ dbt.type_timestamp() }}
        )
    {% endset %}
    {% do run_query(create_table_query) %}
{% endmacro %}