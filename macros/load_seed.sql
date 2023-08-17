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
    quote = '"'
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

{% macro default__load_seed(uri,pattern,compression,headers,null_marker) %}
{% if execute %}
{% do log('No adapter found, seed not loaded',info = True) %}
{% endif %}

{% endmacro %}