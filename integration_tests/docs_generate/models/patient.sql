select
      cast(null as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as first_name
    , cast(null as {{ dbt.type_string() }} ) as last_name
    , cast(null as {{ dbt.type_string() }} ) as sex
    , cast(null as {{ dbt.type_string() }} ) as race
    , cast(null as date) as birth_date
    , cast(null as date) as death_date
    , cast(null as {{ dbt.type_int() }} ) as death_flag
    , cast(null as {{ dbt.type_string() }} ) as address
    , cast(null as {{ dbt.type_string() }} ) as city
    , cast(null as {{ dbt.type_string() }} ) as state
    , cast(null as {{ dbt.type_string() }} ) as zip_code
    , cast(null as {{ dbt.type_string() }} ) as county
    , cast(null as {{ dbt.type_float() }} ) as latitude
    , cast(null as {{ dbt.type_float() }} ) as longitude
    , cast(null as {{ dbt.type_string() }} ) as data_source
limit 0