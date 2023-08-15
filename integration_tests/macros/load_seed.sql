{% macro load_seed(uri) %}
{{ return(adapter.dispatch('load_seed')(uri)) }}
{% endmacro %}



{% macro redshift__load_seed(uri) %}
{% set sql %}
copy  {{ this }}
  from 's3://{{ uri }}'
  access_key_id 'AKIA2EPVNTV4FLAEBFGE'
  secret_access_key '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'
  csv
  region 'us-east-1'
  ignoreheader 1
{% endset %}

{% call statement('redsql',fetch_result=true) %}
{{ sql }}
{% endcall %}

{% if execute %}
{% set results = load_result('redsql') %}
{{ log("Loaded data from external s3 resource\n  loaded to: " ~ this ~ "\n  from: s3://" ~ uri ,True) }}
{% endif %}

{% endmacro %}


{% macro snowflake__load_seed(uri) %}
{% set sql %}
copy into {{ this }}
    from s3://{{ uri }}
    file_format = (type = CSV
    compression = 'none'
    empty_field_as_null = true
    skip_header = 1
    field_optionally_enclosed_by = '"'
    );
{% endset %}

{% call statement('snowsql',fetch_result=true) %}
{{ sql }}
{% endcall %}

{% if execute %}
{% set results = load_result('snowsql') %}
{{ log("Loaded data from external s3 resource\n  loaded to: " ~ this ~ "\n  from: s3://" ~ uri ~ "\n  rows: " ~ results['data'][0][2],True) }}

{% endif %}
{% endmacro %}


{% macro bigquery__load_seed(uri) %}
{%- set columns = adapter.get_columns_in_relation(this) -%}

{% set sql %}
load data into {{ this }} (
    {% for column in columns %}
    {{ column.name }} {{ column.data_type }}{%- if not loop.last %},{% endif %}
    {% endfor %}
)
from files (format = 'csv',
    skip_leading_rows = 1,
    uris = ['gs://{{ uri }}'],
    quote = '"'
    )
{% endset %}

{% call statement('bigsql',fetch_result=true) %}
{{ sql }}
{% endcall %}

{% if execute %}
{% set results = load_result('bigsql') %}
{{ log("Loaded data from external gs resource\n  loaded to: " ~ this ~ "\n  from: gs://" ~ uri ,True) }}
{% endif %}

{% endmacro %}

{% macro default__load_seed(uri) %}
{% if execute %}
{% do log('No adapter found, seed not loaded',info = True) %}
{% endif %}
{% endmacro %}