{#
    This macro includes options for compression, headers, and null markers.
    Default options are set to FALSE. When set to TRUE, the appropriate
    adapter-specific syntax will be used.

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


{% macro duckdb__load_seed(uri,pattern,compression,headers,null_marker) %}
{%- set columns = adapter.get_columns_in_relation(this) -%}
{%- set collist = [] -%}

{% for col in columns %}
  {% do collist.append("'" ~col.name~"'" ~ ": " ~ "'"~col.dtype~"'") %}
{% endfor %}

{%- set cols = collist|join(',') -%}
{# { log( cols,true) } #}

{% set sql %}
  set s3_access_key_id='AKIA2EPVNTV4FLAEBFGE';
  set s3_secret_access_key='TARgblERrFP81Op+52KZW7HrP1Om6ObEDQAUVN2u';
  set s3_region='us-east-1';
  create or replace table {{this}} as
  select
      *
    from
        read_csv('s3://{{ uri }}/{{ pattern }}*',
        {% if null_marker == true %} nullstr = '\N' {% else %} nullstr = '' {% endif %},
         header=true,
         columns= { {{ cols }} } )

{% endset %}

{% call statement('ducksql',fetch_result=true) %}
{{ sql }}
{% endcall %}

{% if execute %}
{# debugging { log(sql, True)} #}
{% set results = load_result('ducksql') %}
{{ log("Loaded data from external s3 resource\n  loaded to: " ~ this ~ "\n  from: s3://" ~ uri ,True) }}
{# debugging { log(results, True) } #}
{% endif %}

{% endmacro %}


{% macro redshift__load_seed(uri,pattern,compression,headers,null_marker) %}
{% set sql %}
copy  {{ this }}
  from 's3://{{ uri }}/{{ pattern }}'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  {% if compression == true %} gzip {% else %} {% endif %}
  {% if headers == true %} ignoreheader 1 {% else %} {% endif %}
  emptyasnull
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
    {% do adapter.load_tuva_seed(this, uri, pattern, compression, headers, null_marker) %}
  {% endif %}
{% endmacro %}


{% macro snowflake__load_seed(uri,pattern,compression,headers,null_marker) %}
{% set sql %}    
copy into {{ this }}
    from s3://{{ uri }}
    file_format = (type = CSV
    {% if compression == true %} compression = 'GZIP' {% else %} compression = 'none' {% endif %}
    {% if headers == true %} skip_header = 1 {% else %} {% endif %}
    empty_field_as_null = true
    field_optionally_enclosed_by = '"'
)
pattern = '.*\/{{pattern}}.*';
{% endset %}
{% call statement('snowsql',fetch_result=true) %}
{{ sql }}
{% endcall %}

{% if execute %}
{# debugging { log(sql, True)} #}
{% set results = load_result('snowsql') %}
{{ log("Loaded data from external s3 resource\n  loaded to: " ~ this ~ "\n  from: s3://" ~ uri ~ "/" ~ pattern ~ "*\n  rows: " ~ results['data']|sum(attribute=2),True) }}
{# debugging { log(results, True)} #}
{% endif %}

{% endmacro %}




{% macro bigquery__load_seed(uri,pattern,compression,headers,null_marker) %}
{%- set columns = adapter.get_columns_in_relation(this) -%}
{%- set collist = [] -%}

{% for col in columns %}
  {% do collist.append(col.name ~ " " ~ col.dtype) %}
{% endfor %}

{%- set cols = collist|join(',') -%}
{# { log( cols,true) } #}
{% set sql %}
load data into {{ this }} ( {{collist|join(',')}} )
from files (format = 'csv',
    uris = ['gs://{{ uri }}/{{ pattern }}*'],
    {% if compression == true %} compression = 'GZIP', {% else %} {% endif %}
    {% if headers == true %} skip_leading_rows = 1, {% else %} {% endif %}
    {% if null_marker == true %} null_marker = '\\N', {% else %} {% endif %}
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
  {% if null_marker == true %} 'nullValue' = '\\N', {% else %} {% endif %}
  'enforceSchema' = 'true',
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



-- postgres - note this requires some pre-work on your part -  you need to clone
-- the data from the tuva public resource bucket to re-assemble it into a single
-- file per seed with quoted null's unquoted - also ensure you've set the
-- Content-Type system header on each to be gzip
-- (https://stackoverflow.com/a/74439053) otherwise the extension won't know to
-- decompress it.
--
-- TODO: revisit removing column list, I think it'll work fine with '' for cols
{% macro postgres__load_seed(uri,pattern,compression,headers,null_marker) %}
{%- set columns = adapter.get_columns_in_relation(this) -%}
{%- set collist = [] -%}
{%- for col in columns -%}
  {%- do collist.append(col.name) -%}
{%- endfor -%}
{%- set cols = collist|join(",") -%}

{%- set s3_bucket = var("tuva_seeds_s3_bucket", uri.split("/")[0]) -%}
{%- set s3_key = uri.split("/")[1:]|join("/") + "/" + pattern + "_0.csv.gz" -%}
{%- if var("tuva_seeds_s3_key_prefix", "") != "" -%}
{%- set s3_key = var("tuva_seeds_s3_key_prefix") + "/" + s3_key -%}
{%- endif -%}
{%- set s3_region = "us-east-1" -%}
{%- set options = ["(", "format csv", ", encoding ''utf8''"] -%}
{%- do options.append(", null ''\\N''") if null_marker == true -%}
{%- do options.append(")") -%}
{%- set options_s = options | join("") -%}

{% set sql %}
SELECT aws_s3.table_import_from_s3(
   '{{ this }}',
   '{{ cols }}',
   '{{ options_s }}',
   aws_commons.create_s3_uri('{{s3_bucket}}', '{{s3_key}}', '{{s3_region}}')
)
{% endset %}

{% call statement('postgressql',fetch_result=true) %}
{{ sql }}
{% endcall %}

{% if execute %}
{# debugging { log(sql, True)} #}
{% set results = load_result('postgressql') %}
{{ log("Loaded data from external s3 resource\n  loaded to: " ~ this ~ "\n  from: s3://" ~ s3_bucket ~ "/" ~ s3_key ,True) }}
{# debugging { log(results, True) } #}
{% endif %}

{% endmacro %}



{% macro default__load_seed(uri,pattern,compression,headers,null_marker) %}
{% if execute %}
{% do log('No adapter found, seed not loaded',info = True) %}
{% endif %}

{% endmacro %}