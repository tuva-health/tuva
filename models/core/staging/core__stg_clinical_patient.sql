{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

SELECT
    cast(patient_id as {{ dbt.type_string() }} ) as patient_id
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
    , cast(FLOOR({{ datediff("cast(birth_date as date)", "DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', '" ~ var('tuva_last_run') ~ "'))", 'day') }} / 365) as {{ dbt.type_int() }}) AS age
    , cast(
        CASE
            WHEN FLOOR({{ datediff("cast(birth_date as date)", "DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', '" ~ var('tuva_last_run') ~ "'))", 'day') }} / 365) < 10 THEN '0-9'
            WHEN FLOOR({{ datediff("cast(birth_date as date)", "DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', '" ~ var('tuva_last_run') ~ "'))", 'day') }} / 365) < 20 THEN '10-19'
            WHEN FLOOR({{ datediff("cast(birth_date as date)", "DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', '" ~ var('tuva_last_run') ~ "'))", 'day') }} / 365) < 30 THEN '20-29'
            WHEN FLOOR({{ datediff("cast(birth_date as date)", "DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', '" ~ var('tuva_last_run') ~ "'))", 'day') }} / 365) < 40 THEN '30-39'
            WHEN FLOOR({{ datediff("cast(birth_date as date)", "DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', '" ~ var('tuva_last_run') ~ "'))", 'day') }} / 365) < 50 THEN '40-49'
            WHEN FLOOR({{ datediff("cast(birth_date as date)", "DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', '" ~ var('tuva_last_run') ~ "'))", 'day') }} / 365) < 60 THEN '50-59'
            WHEN FLOOR({{ datediff("cast(birth_date as date)", "DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', '" ~ var('tuva_last_run') ~ "'))", 'day') }} / 365) < 70 THEN '60-69'
            WHEN FLOOR({{ datediff("cast(birth_date as date)", "DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', '" ~ var('tuva_last_run') ~ "'))", 'day') }} / 365) < 80 THEN '70-79'
            WHEN FLOOR({{ datediff("cast(birth_date as date)", "DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', '" ~ var('tuva_last_run') ~ "'))", 'day') }} / 365) < 90 THEN '80-89'
            ELSE '90+'
        END as {{ dbt.type_string() }}
    ) AS age_group
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run
FROM {{ ref('patient') }}