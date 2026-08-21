{#
  dbt-bigquery does not create a relation for a header-only seed before it
  applies table options. Tuva's committed seed files intentionally contain
  only headers because their rows are loaded from versioned cloud assets in a
  post-hook. Create the typed empty relation here so the normal materialization
  can reach that post-hook.

  Keep the nonempty branch aligned with dbt-bigquery's implementation so this
  override remains safe if the integration project gains an inline seed.
#}

{% macro bigquery__load_csv_rows(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {{ log(
      "TEMP BigQuery seed override selected for " ~ model['name'] ~
      " with " ~ (agate_table.rows | length) ~ " inline rows",
      true
  ) }}

  {% if agate_table.rows | length == 0 %}
    {%- set quote_seed_column = model['config'].get('quote_columns', none) -%}
    {%- set missing_columns = [] -%}
    {%- set extra_columns = [] -%}

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

    {% set create_sql %}
      create or replace table {{ this.render() }} (
        {% for column_name in agate_table.column_names %}
          {{ adapter.quote_seed_column(column_name | string, quote_seed_column) }}
          {{ api.Column.translate_type(column_override[column_name]) }}
          {%- if not loop.last %}, {% endif %}
        {% endfor %}
      )
    {% endset %}

    {% call statement('create_header_only_seed_relation') %}
      {{ create_sql }}
    {% endcall %}
  {% else %}
    {% do adapter.load_dataframe(
        model['database'],
        model['schema'],
        model['alias'],
        agate_table,
        column_override,
        model['config']['delimiter']
    ) %}
  {% endif %}

  {% call statement() %}
    alter table {{ this.render() }} set {{ bigquery_table_options(config, model) }}
  {% endcall %}

  {% if config.persist_relation_docs() and 'description' in model %}
    {% do adapter.update_table_description(
        model['database'],
        model['schema'],
        model['alias'],
        model['description']
    ) %}
  {% endif %}
{% endmacro %}
