{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_columns -%}
      person_id
    , patient_id
    , name_suffix
    , first_name
    , middle_name
    , last_name
    , sex
    , race
    , birth_date
    , death_date
    , death_flag
    , social_security_number
    , address
    , city
    , state
    , zip_code
    , county
    , latitude
    , longitude
    , phone
    , email
    , ethnicity
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , person_id as x_temp_person_id #}
    {# , first_name as x_temp_first_name #}
    {# , last_name as zzz_temp_last_name #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
    , file_name
    , ingest_datetime
{%- endset -%}

{# Uncomment the synthetic extension columns below to test extension columns passthrough feature #}
{%- set tuva_synthetic_extensions -%}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_person_id #}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_first_name #}
    {# , cast(null as {{ dbt.type_string() }}) as zzz_temp_last_name #}
{%- endset -%}

{% if var('use_synthetic_data') == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
cast(null as {{ dbt.type_string() }}) as person_id
, cast(null as {{ dbt.type_string() }}) as patient_id
, cast(null as {{ dbt.type_string() }}) as name_suffix
, cast(null as {{ dbt.type_string() }}) as first_name
, cast(null as {{ dbt.type_string() }}) as middle_name
, cast(null as {{ dbt.type_string() }}) as last_name
, cast(null as {{ dbt.type_string() }}) as sex
, cast(null as {{ dbt.type_string() }}) as race
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as birth_date
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as death_date
, cast(null as {{ dbt.type_int() }}) as death_flag
, cast(null as {{ dbt.type_string() }}) as social_security_number
, cast(null as {{ dbt.type_string() }}) as address
, cast(null as {{ dbt.type_string() }}) as city
, cast(null as {{ dbt.type_string() }}) as state
, cast(null as {{ dbt.type_string() }}) as zip_code
, cast(null as {{ dbt.type_string() }}) as county
, cast(null as {{ dbt.type_float() }}) as latitude
, cast(null as {{ dbt.type_float() }}) as longitude
, cast(null as {{ dbt.type_string() }}) as phone
, cast(null as {{ dbt.type_string() }}) as email
, cast(null as {{ dbt.type_string() }}) as ethnicity
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
from {{ source('source_input', 'patient') }}

{%- endif %}
