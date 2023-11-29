{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
   )
}}

select
    cast(patient_id as {{ dbt.type_string() }} ) as patient_id
    , cast(first_name as {{ dbt.type_string() }} ) as first_name
    , cast(last_name as {{ dbt.type_string() }} ) as last_name
    , cast(sex as {{ dbt.type_string() }} ) as sex
    , cast(race as {{ dbt.type_string() }} ) as race
    , {{ try_to_cast_date('birth_date', 'YYYY-MM-DD') }} as birth_date
    , {{ try_to_cast_date('death_date', 'YYYY-MM-DD') }} as death_date
    , cast(death_flag as {{ dbt.type_int() }} ) as death_flag
    , cast(address as {{ dbt.type_string() }} ) as address
    , cast(city as {{ dbt.type_string() }} ) as city
    , cast(state as {{ dbt.type_string() }} ) as state
    , cast(zip_code as {{ dbt.type_string() }} ) as zip_code
    , cast(county as {{ dbt.type_string() }} ) as county
    , cast(latitude as {{ dbt.type_float() }} ) as latitude
    , cast(longitude as {{ dbt.type_float() }} ) as longitude
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run

from {{ ref('patient') }}