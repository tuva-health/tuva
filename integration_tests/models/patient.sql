select
cast(null as {{ dbt.type_string() }} ) as patient_id
, cast(null as {{ dbt.type_string() }} ) as first_name
, cast(null as {{ dbt.type_string() }} ) as last_name
, cast(null as {{ dbt.type_string() }} ) as sex
, cast(null as {{ dbt.type_string() }} ) as race
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as birth_date
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as death_date
, cast(null as {{ dbt.type_int() }} ) as death_flag
, cast(null as {{ dbt.type_string() }} ) as address
, cast(null as {{ dbt.type_string() }} ) as city
, cast(null as {{ dbt.type_string() }} ) as state
, cast(null as {{ dbt.type_string() }} ) as zip_code
, cast(null as {{ dbt.type_string() }} ) as county
, cast(null as {{ dbt.type_float() }} ) as latitude
, cast(null as {{ dbt.type_float() }} ) as longitude
, cast(null as {{ dbt.type_string() }} ) as data_source
, cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
limit 0