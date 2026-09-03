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
            {# Positional names on the staging table. A Glue table carrying a
               reserved word such as `procedure` cannot be read by CTAS at all,
               so the real names are reapplied in the select below. #}
            {% do column_definitions.append("c" ~ loop.index0 ~ " string" ) %}
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
                    cast(nullif(c{{ loop.index0 }},'{{ null_char }}') as {{ dml_data_type(col.dtype) }}) as "{{ col.name }}" {%-if not loop.last -%},{%- endif %}
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


{#
    SQL Server has no COPY INTO and no wildcard bulk loader, and BULK INSERT
    cannot decompress. It does have a native GZIP builtin, so the published
    .csv.gz is read as a single blob, decompressed in-engine, split into lines
    and parsed field-wise.

    The parse is RFC 4180 over the assets exactly as published, with their
    existing minimal quoting: only fields that need quotes carry them. Splitting
    a line on every comma over-splits any quoted field containing one, so tokens
    are re-joined wherever the running count of double quotes before them is
    odd, which is precisely when the preceding comma fell inside a quoted field.

    Parsing what is already published matters more than a simpler parser would.
    Requiring fully-quoted CSV instead would turn every bare empty field into a
    quoted empty string, and several loaders treat only the bare form as NULL,
    so republishing to suit this adapter risks changing NULL semantics for the
    warehouses that already work.

    tuva_seed_sqlserver_storage_root accepts either an https blob endpoint
    (the default public mirror) or a local filesystem root for testing.
#}

{% macro sqlserver__load_seed(uri, pattern, compression, headers, null_marker) %}
{% if execute %}
{% do the_tuva_project.reset_seed_relation() %}

{%- set columns = adapter.get_columns_in_relation(this) -%}
{%- if columns | length == 0 -%}
  {% do exceptions.raise_compiler_error("SQL Server seed load found no columns on " ~ this ~ ".") %}
{%- endif -%}

{%- set storage_root = var('tuva_seed_sqlserver_storage_root', 'https://tuvapublicresources.blob.core.windows.net') | string | trim -%}
{%- set storage_root = storage_root.rstrip('/') -%}
{%- set is_remote = storage_root.startswith('http://') or storage_root.startswith('https://') -%}
{%- set normalized_uri = uri | string | trim | trim('/') -%}

{%- if is_remote -%}
  {#- The first uri segment is the storage container; the rest is the blob path. -#}
  {%- set uri_parts = normalized_uri.split('/') -%}
  {%- set container = uri_parts[0] -%}
  {%- set blob_path = uri_parts[1:] | join('/') -%}
  {%- set data_source_name = 'tuva_seed_' ~ container | replace('-', '_') | replace('.', '_') -%}
  {%- set object_path = blob_path ~ '/' ~ pattern -%}
  {%- set source_args = "'" ~ object_path ~ "', data_source = '" ~ data_source_name ~ "', single_blob" -%}
  {#
      Seeds run concurrently, so two threads can both pass the existence check
      before either creates the data source. Swallow the resulting duplicate
      error and re-check, rather than letting a harmless race fail the load.
  #}
  {% set ensure_source %}
    if not exists (select 1 from sys.external_data_sources where name = '{{ data_source_name }}')
    begin
      begin try
        exec('create external data source [{{ data_source_name }}] with (type = BLOB_STORAGE, location = ''{{ storage_root }}/{{ container }}'')');
      end try
      begin catch
        if not exists (select 1 from sys.external_data_sources where name = '{{ data_source_name }}')
          throw;
      end catch
    end
  {% endset %}
  {% call statement('sqlserver_seed_source', fetch_result=false) %}{{ ensure_source }}{% endcall %}
{%- else -%}
  {%- set object_path = storage_root ~ '/' ~ normalized_uri ~ '/' ~ pattern -%}
  {%- set source_args = "'" ~ object_path ~ "', single_blob" -%}
{%- endif -%}

{#- DECOMPRESS is SQL Server's GZIP builtin; skip it for uncompressed sources. -#}
{%- if compression == true -%}
  {%- set payload = "cast(decompress(src.BulkColumn) as varchar(max))" -%}
{%- else -%}
  {%- set payload = "cast(src.BulkColumn as varchar(max))" -%}
{%- endif -%}

{%- set insert_cols = [] -%}
{%- set select_exprs = [] -%}
{%- set pivot_exprs = [] -%}
{%- for col in columns -%}
  {%- do insert_cols.append(the_tuva_project.quote_column(col.name)) -%}
  {%- do pivot_exprs.append("max(case when field_no = " ~ loop.index ~ " then val end) as c" ~ loop.index0) -%}
  {#- cast(x as varchar) with no length silently truncates to 30 characters in
      T-SQL, and get_columns_in_relation reports varchar(max) as a bare
      "varchar". Always render an explicit length. -#}
  {%- set dt = col.dtype | lower | trim -%}
  {%- if 'char' in dt and '(' not in dt -%}
    {%- set size = col.char_size -%}
    {%- set target_type = dt ~ '(' ~ ('max' if (size is none or size <= 0) else size) ~ ')' -%}
  {%- else -%}
    {%- set target_type = col.dtype -%}
  {%- endif -%}
  {%- set raw = "nullif(parsed.c" ~ loop.index0 ~ ", '__TUVA_RESERVED_NULL_MARKER_1_0__')" -%}
  {%- do select_exprs.append("cast(" ~ raw ~ " as " ~ target_type ~ ")") -%}
{%- endfor -%}

{% set sql %}
insert into {{ this }} ({{ insert_cols | join(', ') }})
select
      {{ select_exprs | join('\n    , ') }}
from (
    select
          line_no
        , {{ pivot_exprs | join('\n        , ') }}
    from (
        /* Strip the surrounding quotes from a quoted field and undouble its
           internal quotes. len() ignores trailing spaces, so measure with a
           sentinel appended. An empty field is NULL, matching the bare-empty
           null convention the assets are published with. */
        select
              line_no
            , field_no
            , nullif(
                  case
                      when (len(rf + '|') - 1) >= 2 and left(rf, 1) = '"' and right(rf, 1) = '"'
                          then replace(substring(rf, 2, (len(rf + '|') - 1) - 2), '""', '"')
                      else rf
                  end
              , '') as val
        from (
            select
                  line_no
                , field_no
                , string_agg(cast(tok as varchar(max)), ',') within group (order by tok_no) as rf
            from (
                /* A comma only ends a field when an even number of quotes has
                   been seen so far on the line; otherwise it is inside a
                   quoted field and its tokens belong to the same field. */
                select
                      line_no
                    , tok_no
                    , tok
                    , 1 + isnull(
                          sum(case when cum % 2 = 0 then 1 else 0 end) over (
                              partition by line_no order by tok_no
                              rows between unbounded preceding and 1 preceding
                          ), 0) as field_no
                from (
                    select
                          line_no
                        , tok_no
                        , tok
                        , sum(quote_count) over (
                              partition by line_no order by tok_no
                              rows between unbounded preceding and current row
                          ) as cum
                    from (
                        select
                              ln.ordinal as line_no
                            , t.ordinal as tok_no
                            , t.value as tok
                            , (len(t.value + '|') - 1) - (len(replace(t.value, '"', '') + '|') - 1) as quote_count
                        from openrowset(bulk {{ source_args }}) as src
                        cross apply (select replace({{ payload }}, char(13), '') as text) as body
                        cross apply string_split(body.text, char(10), 1) as ln
                        cross apply string_split(ln.value, ',', 1) as t
                        where ln.ordinal > {{ 1 if headers == true else 0 }}
                          and len(ln.value + '|') > 1
                    ) as tokens
                ) as running
            ) as fields
            group by line_no, field_no
        ) as grouped
    ) as cleaned
    group by line_no
) as parsed
{% endset %}

{% call statement('sqlserver_seed', fetch_result=false) %}{{ sql }}{% endcall %}

{% set count_sql %}select count(*) as row_count from {{ this }}{% endset %}
{% call statement('sqlserver_seed_count', fetch_result=true) %}{{ count_sql }}{% endcall %}
{% set count_result = load_result('sqlserver_seed_count') %}
{% set row_count = count_result.table.columns[0].values()[0] if count_result.table else 0 %}
{{ log("Loaded data from " ~ ("external blob storage" if is_remote else "local storage")
       ~ "\n  loaded to: " ~ this ~ "\n  from: " ~ object_path ~ "\n  rows: " ~ row_count, True) }}

{% endif %}
{% endmacro %}


{% macro default__load_seed(uri,pattern,compression,headers,null_marker) %}
{% if execute %}
{% do log('No adapter found, seed not loaded',info = True) %}
{% endif %}

{% endmacro %}
