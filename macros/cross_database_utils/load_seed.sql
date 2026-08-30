{#
    This macro includes options for compression, headers, and null markers.
    Package-aligned assets are canonicalized to bare empty null fields before
    publication and call this macro with null_marker=true. The null_marker
    argument remains in the public signature for compatibility; native CSV
    null behavior means false does not preserve empty strings on every adapter.

    Argument examples:
    compression=false
    compression=true
    headers=false
    headers=true
    null_marker=false
    null_marker=true
#}

{% macro load_seed(uri,pattern,compression=false,headers=false,null_marker=false) %}
{{ return(adapter.dispatch('load_seed', 'the_tuva_project')(uri,pattern,compression,headers,null_marker)) }}
{% endmacro %}


{% macro reset_seed_relation() %}
{{ return(adapter.dispatch('reset_seed_relation', 'the_tuva_project')()) }}
{% endmacro %}


{% macro default__reset_seed_relation() %}
{% set sql %}
truncate table {{ this }}
{% endset %}

{% call statement('seed_reset', fetch_result=false) %}
{{ sql }}
{% endcall %}
{% endmacro %}


{% macro duckdb__reset_seed_relation() %}
{% endmacro %}


{% macro athena__reset_seed_relation() %}
{% endmacro %}


{% macro duckdb__load_seed(uri,pattern,compression,headers,null_marker) %}
{%- set columns = adapter.get_columns_in_relation(this) -%}
{%- set collist = [] -%}
{%- set local_storage_root = var('tuva_seed_duckdb_storage_root', '') | string | trim -%}

{% if local_storage_root == '' %}
  {%- set seed_path = 's3://' ~ uri ~ '/' ~ pattern ~ '*' -%}
{% else %}
  {%- set root_separator = '' if local_storage_root.endswith('/') else '/' -%}
  {%- set seed_path = local_storage_root ~ root_separator ~ uri ~ '/' ~ pattern ~ '*' -%}
{% endif %}

{% for col in columns %}
  {% do collist.append("'" ~col.name~"'" ~ ": " ~ "'"~col.dtype~"'") %}
{% endfor %}

{%- set cols = collist|join(',') -%}
{# { log( cols,true) } #}

{% set sql %}
  create or replace table {{this}} as
  select
      *
    from
        read_csv('{{ seed_path | replace("'", "''") }}',
        nullstr = '',
         quote = '"', escape = '"',
         header={{headers}},
         columns= { {{ cols }} } )

{% endset %}

{% call statement('ducksql',fetch_result=true) %}
{{ sql }}
{% endcall %}

{% set count_sql %}
  SELECT COUNT(*) AS row_count FROM {{ this }}
{% endset %}

{% call statement('count',fetch_result=true) %}
  {{ count_sql }}
{% endcall %}

{% if execute %}
{# debugging { log(sql, True)} #}
{% set count_result = load_result('count') %}
{% set row_count = count_result.table.columns[0].values()[0] if count_result.table else 0 %}
{{ log("Loaded data from external resource\n  loaded to: " ~ this ~ "\n  from: " ~ seed_path ~ "\n  rows: " ~ row_count,True) }}
{# debugging { log(results, True) } #}
{% endif %}

{% endmacro %}


{% macro redshift__load_seed(uri,pattern,compression,headers,null_marker) %}
{% do the_tuva_project.reset_seed_relation() %}
{% set sql %}

copy  {{ this }}
  from 's3://{{ uri }}/{{ pattern }}'
    iam_role default
  csv
  {% if compression == true %} gzip {% else %} {% endif %}
  {% if headers == true %} ignoreheader 1 {% else %} {% endif %}
  emptyasnull
  /* Redshift's default NULL AS value is \\N. Override it with the
     publisher-reserved sentinel so quoted literal \\N remains text. */
  null as '__TUVA_RESERVED_NULL_MARKER_1_0__'
  region 'us-east-1'

{% endset %}

{% call statement('redsql',fetch_result=true) %}
{{ sql }}
{% endcall %}

{% if execute %}
{# debugging { log(sql, True)} #}
{% set results = load_result('redsql') %}
{{ log("Loaded data from external s3 resource\n  loaded to: " ~ this ~ "\n  from: s3://" ~ uri ,True) }}
{# debugging { log(results, True) } #}
{% endif %}
{% endmacro %}


{% macro athena__load_seed(uri, pattern, compression, headers, null_marker) %}
  {% if execute %}
        {%- set columns = adapter.get_columns_in_relation(this) -%}
        {%- set column_definitions = [] -%}
        {%- set null_char = '' -%}

        {% for col in columns %}
            {% do column_definitions.append(col.name ~ " string" ) %}
        {% endfor %}

        {%- set col_ddl = column_definitions|join(',') -%}

        {% set bucket = 's3://' ~ uri ~ '/' %}
        {% set full_path = bucket  ~ pattern %}
        {% set table_name = this.schema ~ '.' ~  this.name %}
        {% set tmp_table = this.schema ~ '.' ~  this.name ~ "__dbt_tmp_external" %}
        {% set header_line_count %}{% if headers -%}1{%- else -%}0{%- endif -%}{% endset %}


        {% set drop_tmp_table %}
            DROP TABLE IF EXISTS `{{ tmp_table }}`;
        {% endset %}
        {% set create_tmp_table %}
            CREATE EXTERNAL TABLE `{{ tmp_table }}` ( {{ col_ddl }} )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
                'separatorChar' = ',',
                'quoteChar' = '"',
                /* Disable OpenCSV's default backslash escape. Otherwise a
                   quoted literal \\N loses its backslash before nullif. */
                'escapeChar' = '\0'
            )
            STORED AS TEXTFILE
            LOCATION '{{ bucket }}'
            TBLPROPERTIES ('skip.header.line.count'='{{ header_line_count }}', 'compressionType'='GZIP');
        {% endset %}

        {% set drop_seed_table %}
            DROP TABLE IF EXISTS `{{ table_name }}`;
        {% endset %}
        {% set create_seed_table %}
            CREATE TABLE {{ table_name }} AS
                SELECT
                {% for col in columns %}
                    cast(nullif({{ col.name }},'{{ null_char }}') as {{ dml_data_type(col.dtype) }}) as {{ col.name }} {%-if not loop.last -%},{%- endif %}
                {% endfor %}
                FROM {{ tmp_table }}
                WHERE "$path" like '{{ full_path }}%';
        {% endset %}

        {% for query in [drop_tmp_table, create_tmp_table, drop_seed_table, create_seed_table, drop_tmp_table] %}
            {% call statement('stage', fetch_result=true) %}
                {{ query }}
            {% endcall %}
        {% endfor %}

  {% endif %}
{% endmacro %}


{% macro snowflake_seed_rows_loaded(column_names, data, uri, pattern) %}
{% set normalized_column_names = [] %}
{% for column_name in column_names %}
  {% do normalized_column_names.append(column_name | string | lower) %}
{% endfor %}

{% if data | length == 0 %}
  {% do exceptions.raise_compiler_error(
      "Snowflake seed load returned no result for s3://" ~ uri ~ "/" ~ pattern ~ "."
  ) %}
{% endif %}

{% if 'rows_loaded' not in normalized_column_names %}
  {% set copy_status = data[0][0] if data[0] | length > 0 else 'COPY returned no status' %}
  {% do exceptions.raise_compiler_error(
      "Snowflake seed load processed no files from s3://"
      ~ uri ~ "/" ~ pattern ~ ". Result: " ~ copy_status
  ) %}
{% endif %}

{% set rows_loaded_index = normalized_column_names.index('rows_loaded') %}
{% set loaded = namespace(rows=0) %}
{% for row in data %}
  {% set loaded.rows = loaded.rows + (row[rows_loaded_index] | int) %}
{% endfor %}

{% if loaded.rows == 0 %}
  {% set statuses = [] %}
  {% if 'status' in normalized_column_names %}
    {% set status_index = normalized_column_names.index('status') %}
    {% for row in data %}
      {% do statuses.append(row[status_index] | string) %}
    {% endfor %}
  {% endif %}
  {% set copy_status = statuses | join(', ') if statuses | length > 0 else 'COPY loaded zero rows' %}
  {% do exceptions.raise_compiler_error(
      "Snowflake seed load loaded zero rows from s3://"
      ~ uri ~ "/" ~ pattern ~ ". Result: " ~ copy_status
  ) %}
{% endif %}

{{ return(loaded.rows) }}
{% endmacro %}


{% macro snowflake__load_seed(uri,pattern,compression,headers,null_marker) %}
{% do the_tuva_project.reset_seed_relation() %}
{% set sql %}
copy into {{ this }}
    from s3://{{ uri }}
    file_format = (
      type = CSV
      {% if compression == true %} compression = 'GZIP' {% else %} compression = 'none' {% endif %}
      {% if headers == true %} skip_header = 1
      {% endif %}
      empty_field_as_null = true
      field_optionally_enclosed_by = '"'
      escape_unenclosed_field = NONE
      /* Bare empty fields are handled by empty_field_as_null. This reserved
         value neutralizes Snowflake's default \\N marker without colliding
         with a publisher-approved data value. */
      null_if = ('__TUVA_RESERVED_NULL_MARKER_1_0__')
)
pattern = '.*\/{{pattern}}.*';
{% endset %}
{% call statement('snowsql',fetch_result=true) %}
{{ sql }}
{% endcall %}

{% if execute %}
{# debugging { log(sql, True)} #}
{% set results = load_result('snowsql') %}
{% set rows_loaded = the_tuva_project.snowflake_seed_rows_loaded(
    results['table'].column_names,
    results['data'],
    uri,
    pattern
) %}
{{ log("Loaded data from external s3 resource\n  loaded to: " ~ this ~ "\n  from: s3://" ~ uri ~ "/" ~ pattern ~ "*\n  rows: " ~ rows_loaded,True) }}
{# debugging { log(results, True)} #}
{% endif %}

{% endmacro %}




{% macro bigquery__load_seed(uri,pattern,compression,headers,null_marker) %}
{% do the_tuva_project.reset_seed_relation() %}
{%- set columns = adapter.get_columns_in_relation(this) -%}
{%- set collist = [] -%}

{% for col in columns %}
  {% do collist.append(col.name ~ " " ~ col.dtype) %}
{% endfor %}

{% set sql %}
load data into {{ this }} ( {{ collist|join(',') }} )
from files (format = 'csv',
    uris = ['gs://{{ uri }}/{{ pattern }}*'],
    {% if compression == true %} compression = 'GZIP', {% else %} {% endif %}
    {% if headers == true %} skip_leading_rows = 1, {% else %} {% endif %}
    {% if null_marker == true %} null_markers = [''], {% else %} {% endif %}
    quote = '"',
    allow_quoted_newlines = True
    )
{% endset %}

{% call statement('bigsql',fetch_result=true) %}
{{ sql }}
{% endcall %}

{% if execute %}
{# { log(sql, True) } #}
{% set results = load_result('bigsql') %}
{{ log("Loaded data from external gs resource\n  loaded to: " ~ this ~ "\n  from: gs://" ~ uri ~ "/" ~ pattern ~ "*",True) }}
{# log(results, True) #}
{% endif %}

{% endmacro %}



{% macro databricks__load_seed(uri,pattern,compression,headers,null_marker) %}
{% if execute %}
{% do the_tuva_project.reset_seed_relation() %}

{%- set s3_path = 's3://' ~ uri ~ '/' -%}
{%- set columns = adapter.get_columns_in_relation(this) -%}
{%- set collist = [] -%}

{% for col in columns %}
  {% do collist.append("_c" ~ loop.index0 ~ "::" ~ col.dtype ~ " AS " ~ col.name ) %}
{% endfor %}

{%- set cols = collist|join(',\n    ') -%}

{% set sql %}
COPY INTO {{ this }}
FROM (
  SELECT
    {{ cols }}

  FROM '{{ s3_path }}'
  {% if env_var('AWS_SESSION_TOKEN', False) %}
  WITH (
    CREDENTIAL (
      AWS_ACCESS_KEY = "{{ env_var('AWS_ACCESS_KEY_ID') }}",
      AWS_SECRET_KEY = "{{ env_var('AWS_SECRET_ACCESS_KEY') }}",
      AWS_SESSION_TOKEN = "{{ env_var('AWS_SESSION_TOKEN') }}"
    )
  )
  {% endif %}
)
FILEFORMAT = CSV
PATTERN = '{{ pattern }}*'
FORMAT_OPTIONS (
  {% if headers == true %} 'skipRows' = '1', {% else %} 'skipRows' = '0', {% endif %}
  {% if null_marker == true %} 'nullValue' = '', {% else %} {% endif %}
  'enforceSchema' = 'false',
  'inferSchema' = 'false',
  'sep' = ',',
  'escape' = "\"",
  'multiline' = 'true'
)
COPY_OPTIONS (
  'mergeSchema' = 'false',
  'force' = 'true'
)
{% endset %}

{# check logs/dbt.log for output #}
{{ log(cols, info=False) }}
{{ log('Current model: ' ~ this ~ '\n', info=False) }}
{{ log('Full s3 path: ' ~ s3_path ~ '\n', info=False) }}
{{ log(sql, info=False) }}

{% call statement('databrickssql',fetch_result=true) %}
{{ sql }}
{% endcall %}

{% set results = load_result('databrickssql') %}
{% set rows_affected = results['data'][0][0] %}

{{ log(results, info=False) }}
{{ log(rows_affected, info=False) }}

{{ log("Loaded data from external s3 resource:", True) }}
{{ log("  source: \t" ~ s3_path ~ pattern, True) }}
{{ log("  target: \t" ~ this | replace('`',''), True) }}
{{ log("  rows: \t\033[92m" ~ rows_affected ~ "\033[0m", True) }}

{% endif %}
{% endmacro %}

{% macro fabric__load_seed(uri, pattern, compression, headers, null_marker) %}
{% do the_tuva_project.reset_seed_relation() %}
{% set fabric_storage_root = var('tuva_seed_fabric_storage_root', 'https://tuvapublicresources.blob.core.windows.net') | trim('/') %}
{% set sql %}
COPY INTO {{ this }}
FROM '{{ fabric_storage_root }}/{{ uri }}/{{ pattern }}'
WITH (
    FILE_TYPE = 'CSV'
    , ENCODING = 'UTF8'
    , FIELDQUOTE = '"'
    , ROWTERMINATOR = '0x0A'
    {% if compression == true %}, COMPRESSION = 'GZIP' {% else %} {% endif %}
    {% if headers == true %}, FIRSTROW = 2 {% else %} {% endif %}
);
{% endset %}
{% call statement('fabricsql', fetch_result=true) %}
{{ sql }}
{% endcall %}

{% if execute %}
{# debugging { log(sql, True)} #}
{% set results = load_result('fabricsql') %}
{% set rows_loaded = results['response'].rows_affected|default(0) %}
{{ log("Loaded data from external Azure Blob Storage\n  loaded to: " ~ this ~ "\n  from: " ~ uri ~ "/" ~ pattern ~ "\n  rows: " ~ rows_loaded, True) }}
{# debugging { log(results, True)} #}
{% endif %}

{% endmacro %}


{% macro default__load_seed(uri,pattern,compression,headers,null_marker) %}
{% if execute %}
{% do log('No adapter found, seed not loaded',info = True) %}
{% endif %}

{% endmacro %}
