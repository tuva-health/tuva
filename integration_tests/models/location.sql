{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_columns -%}
      location_id
    , npi
    , name
    , facility_type
    , parent_organization
    , address
    , city
    , state
    , zip_code
    , latitude
    , longitude
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , state as x_temp_state #}
    {# , parent_organization as x_temp_parent_organization #}
    {# , facility_type as zzz_temp_facility_type #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
    , file_name
    , ingest_datetime
{%- endset -%}

{# Uncomment the synthetic extension columns below to test extension columns passthrough feature #}
{%- set tuva_synthetic_extensions -%}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_state #}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_parent_organization #}
    {# , cast(null as {{ dbt.type_string() }}) as zzz_temp_facility_type #}
{%- endset -%}

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
{{ tuva_synthetic_extensions }}
, cast(null as {{ dbt.type_string() }}) as data_source
, cast(null as {{ dbt.type_string() }}) as file_name
, cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
{{ limit_zero() }}

{%- else -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ source('source_input', 'location') }}

{%- endif %}
