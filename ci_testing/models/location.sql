{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{% if var('use_synthetic_data') == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
cast(null as {{ dbt.type_string() }}) as location_id
, cast(null as {{ dbt.type_string() }}) as npi
, cast(null as {{ dbt.type_string() }}) as name
, cast(null as {{ dbt.type_string() }}) as facility_type
, cast(null as {{ dbt.type_string() }}) as parent_organization
, cast(null as {{ dbt.type_string() }}) as address
, cast(null as {{ dbt.type_string() }}) as city
, cast(null as {{ dbt.type_string() }}) as state
, cast(null as {{ dbt.type_string() }}) as zip_code
, cast(null as {{ dbt.type_float() }}) as latitude
, cast(null as {{ dbt.type_float() }}) as longitude
, cast(null as {{ dbt.type_string() }}) as data_source
, cast(null as {{ dbt.type_string() }}) as file_name
, cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
{{ limit_zero() }}

{%- else -%}

select * from {{ source('source_input', 'location') }}

{%- endif %}
