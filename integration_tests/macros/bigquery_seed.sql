{#
  dbt-bigquery treats table creation as a no-op and drops an existing table
  during seed reset. That works when dbt subsequently uploads inline rows, but
  dbt-core 1.10 skips row loading for Tuva's intentionally header-only seed
  files. Create the typed empty relation in both paths so the normal Tuva
  post-hook can replace it with the versioned cloud asset.
#}

{% macro bigquery_create_seed_relation(model, agate_table, relation) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {{ log(
      "TEMP BigQuery seed relation override selected for " ~ model['name'] ~
      " with " ~ (agate_table.rows | length) ~ " inline rows",
      true
  ) }}
  {%- set quote_seed_column = model['config'].get('quote_columns', none) -%}
  {%- set missing_columns = [] -%}
  {%- set extra_columns = [] -%}
  {%- set column_definitions = [] -%}

  {% if agate_table.rows | length == 0 %}
    {% for column_name in agate_table.column_names %}
      {% if not column_override.get(column_name) %}
        {% do missing_columns.append(column_name) %}
      {% endif %}
    {% endfor %}

    {% for column_name in column_override %}
      {% if column_name not in agate_table.column_names %}
        {% do extra_columns.append(column_name) %}
      {% endif %}
    {% endfor %}

    {% if missing_columns or extra_columns %}
      {{ exceptions.raise_compiler_error(
          "Header-only BigQuery seed " ~ model['name'] ~
          " requires column_types that exactly match its CSV header. " ~
          "Missing: " ~ (missing_columns | join(', ')) ~ "; extra: " ~
          (extra_columns | join(', '))
      ) }}
    {% endif %}
  {% endif %}

  {% for column_name in agate_table.column_names %}
    {%- set configured_type = column_override.get(column_name) -%}
    {%- set column_type = configured_type
        if configured_type
        else adapter.convert_type(agate_table, loop.index0) -%}
    {% do column_definitions.append(
        adapter.quote_seed_column(column_name | string, quote_seed_column) ~
        " " ~ api.Column.translate_type(column_type)
    ) %}
  {% endfor %}

  {% set create_sql %}
    create or replace table {{ relation.render() }} (
      {{ column_definitions | join(',\n      ') }}
    )
  {% endset %}

  {% call statement('create_bigquery_seed_relation') %}
    {{ create_sql }}
  {% endcall %}

  {{ return(create_sql) }}
{% endmacro %}


{% macro bigquery__create_csv_table(model, agate_table) %}
  {{ return(bigquery_create_seed_relation(model, agate_table, this)) }}
{% endmacro %}


{% macro bigquery__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
  {% do adapter.drop_relation(old_relation) %}
  {{ return(bigquery_create_seed_relation(model, agate_table, this)) }}
{% endmacro %}
