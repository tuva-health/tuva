{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

{%- set tuva_core_columns -%}
      cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(patient_id as {{ dbt.type_string() }}) as patient_id
    , cast(name_suffix as {{ dbt.type_string() }}) as name_suffix
    , cast(first_name as {{ dbt.type_string() }}) as first_name
    , cast(middle_name as {{ dbt.type_string() }}) as middle_name
    , cast(last_name as {{ dbt.type_string() }}) as last_name
    , cast(sex as {{ dbt.type_string() }}) as sex
    , cast(race as {{ dbt.type_string() }}) as race
    , {{ try_to_cast_date('birth_date', 'YYYY-MM-DD') }} as birth_date
    , {{ try_to_cast_date('death_date', 'YYYY-MM-DD') }} as death_date
    , cast(death_flag as {{ dbt.type_int() }}) as death_flag
    , cast(social_security_number as {{ dbt.type_string() }}) as social_security_number
    , cast(address as {{ dbt.type_string() }}) as address
    , cast(city as {{ dbt.type_string() }}) as city
    , cast(state as {{ dbt.type_string() }}) as state
    , cast(zip_code as {{ dbt.type_string() }}) as zip_code
    , cast(county as {{ dbt.type_string() }}) as county
    , cast(latitude as {{ dbt.type_numeric() }}) as latitude
    , cast(longitude as {{ dbt.type_numeric() }}) as longitude
    , cast(phone as {{ dbt.type_string() }}) as phone
    , cast(email as {{ dbt.type_string() }}) as email
    , cast(ethnicity as {{ dbt.type_string() }}) as ethnicity
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('input_layer__patient'), strip_prefix=false) }}
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(file_name as {{ dbt.type_string() }}) as file_name
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('input_layer__patient') }}
