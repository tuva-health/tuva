{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

with tuva_last_run as(
    select
       cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run_datetime
       , cast(substring('{{ var('tuva_last_run')}}',1,10) as date ) as tuva_last_run_date
)
SELECT
      cast(person_id as {{ dbt.type_string() }} ) as person_id
    , cast(first_name as {{ dbt.type_string() }} ) as first_name
    , cast(last_name as {{ dbt.type_string() }} ) as last_name
    , cast(sex as {{ dbt.type_string() }} ) as sex
    , cast(race as {{ dbt.type_string() }} ) as race
    , {{ try_to_cast_date('birth_date', 'YYYY-MM-DD') }} as birth_date
    , {{ try_to_cast_date('death_date', 'YYYY-MM-DD') }} as death_date
    , cast(death_flag as {{ dbt.type_int() }} ) as death_flag
    , cast(social_security_number as {{ dbt.type_string() }} ) as social_security_number
    , cast(address as {{ dbt.type_string() }} ) as address
    , cast(city as {{ dbt.type_string() }} ) as city
    , cast(state as {{ dbt.type_string() }} ) as state
    , cast(zip_code as {{ dbt.type_string() }} ) as zip_code
    , cast(county as {{ dbt.type_string() }} ) as county
    , cast(latitude as {{ dbt.type_float() }} ) as latitude
    , cast(longitude as {{ dbt.type_float() }} ) as longitude
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast(floor({{ datediff('birth_date', 'tuva_last_run_date', 'hour') }} / 8760.0) as {{ dbt.type_int() }} ) as age
    , cast(
        CASE
            WHEN cast(floor({{ datediff('birth_date', 'tuva_last_run_date', 'hour') }} / 8760.0) as {{ dbt.type_int() }} ) < 10 THEN '0-9'
            WHEN cast(floor({{ datediff('birth_date', 'tuva_last_run_date', 'hour') }} / 8760.0) as {{ dbt.type_int() }} ) < 20 THEN '10-19'
            WHEN cast(floor({{ datediff('birth_date', 'tuva_last_run_date', 'hour') }} / 8760.0) as {{ dbt.type_int() }} ) < 30 THEN '20-29'
            WHEN cast(floor({{ datediff('birth_date', 'tuva_last_run_date', 'hour') }} / 8760.0) as {{ dbt.type_int() }} ) < 40 THEN '30-39'
            WHEN cast(floor({{ datediff('birth_date', 'tuva_last_run_date', 'hour') }} / 8760.0) as {{ dbt.type_int() }} ) < 50 THEN '40-49'
            WHEN cast(floor({{ datediff('birth_date', 'tuva_last_run_date', 'hour') }} / 8760.0) as {{ dbt.type_int() }} ) < 60 THEN '50-59'
            WHEN cast(floor({{ datediff('birth_date', 'tuva_last_run_date', 'hour') }} / 8760.0) as {{ dbt.type_int() }} ) < 70 THEN '60-69'
            WHEN cast(floor({{ datediff('birth_date', 'tuva_last_run_date', 'hour') }} / 8760.0) as {{ dbt.type_int() }} ) < 80 THEN '70-79'
            WHEN cast(floor({{ datediff('birth_date', 'tuva_last_run_date', 'hour') }} / 8760.0) as {{ dbt.type_int() }} ) < 90 THEN '80-89'
            ELSE '90+'
        END as {{ dbt.type_string() }}
    ) AS age_group
    , tuva_last_run_datetime as tuva_last_run
FROM {{ ref('patient') }}
cross join tuva_last_run