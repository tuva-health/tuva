{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

{%- set eligibility_extension_columns -%}
    {{ select_extension_columns(ref('normalized__eligibility'), alias='elig', strip_prefix=false) }}
{%- endset -%}

{%- set final_extension_columns -%}
    {{ select_extension_columns(ref('normalized__eligibility'), alias='eligibility_source', strip_prefix=false) }}
{%- endset -%}

with eligibility_source as (
    select
          cast(elig.person_id as {{ dbt.type_string() }}) as person_id
        , cast(elig.name_suffix as {{ dbt.type_string() }}) as name_suffix
        , cast(elig.first_name as {{ dbt.type_string() }}) as first_name
        , cast(elig.middle_name as {{ dbt.type_string() }}) as middle_name
        , cast(elig.last_name as {{ dbt.type_string() }}) as last_name
        , cast(elig.gender as {{ dbt.type_string() }}) as sex
        , cast(elig.race as {{ dbt.type_string() }}) as race
        , elig.birth_date as birth_date
        , elig.death_date as death_date
        , cast(elig.death_flag as {{ dbt.type_int() }}) as death_flag
        , cast(elig.social_security_number as {{ dbt.type_string() }}) as social_security_number
        , cast(elig.address as {{ dbt.type_string() }}) as address
        , cast(elig.city as {{ dbt.type_string() }}) as city
        , cast(elig.state as {{ dbt.type_string() }}) as state
        , cast(elig.zip_code as {{ dbt.type_string() }}) as zip_code
        , cast(null as {{ dbt.type_string() }}) as county
        , cast(null as {{ dbt.type_numeric() }}) as latitude
        , cast(null as {{ dbt.type_numeric() }}) as longitude
        , cast(elig.phone as {{ dbt.type_string() }}) as phone
        , cast(elig.email as {{ dbt.type_string() }}) as email
        , cast(elig.ethnicity as {{ dbt.type_string() }}) as ethnicity
        {{ eligibility_extension_columns }}
        , cast(elig.data_source as {{ dbt.type_string() }}) as data_source
        , cast(elig.file_name as {{ dbt.type_string() }}) as file_name
        , cast(elig.ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
        , elig.tuva_last_run as tuva_last_run
        , row_number() over (
            partition by elig.person_id, elig.data_source
            order by case
                when elig.enrollment_end_date is null then cast('2050-01-01' as date)
                else elig.enrollment_end_date
            end desc
        ) as row_sequence
    from {{ ref('normalized__eligibility') }} as elig
)

select
      eligibility_source.person_id
    , eligibility_source.name_suffix
    , eligibility_source.first_name
    , eligibility_source.middle_name
    , eligibility_source.last_name
    , eligibility_source.sex
    , eligibility_source.race
    , eligibility_source.birth_date
    , eligibility_source.death_date
    , eligibility_source.death_flag
    , eligibility_source.social_security_number
    , eligibility_source.address
    , eligibility_source.city
    , eligibility_source.state
    , eligibility_source.zip_code
    , eligibility_source.county
    , eligibility_source.latitude
    , eligibility_source.longitude
    , eligibility_source.phone
    , eligibility_source.email
    , eligibility_source.ethnicity
    {{ final_extension_columns }}
    , eligibility_source.data_source
    , eligibility_source.file_name
    , eligibility_source.ingest_datetime
    , eligibility_source.tuva_last_run
from eligibility_source
where row_sequence = 1
