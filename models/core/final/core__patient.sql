{{ config(
     enabled = var('core_enabled',var('tuva_marts_enabled',True))
   )
}}

-- *************************************************
-- This dbt model creates the patient table in core.
-- *************************************************




with patient_stage as(
    select
        patient_id
        ,gender
        ,race
        ,birth_date
        ,death_date
        ,death_flag
        ,first_name
        ,last_name
        ,address
        ,city
        ,state
        ,zip_code
        ,phone
        ,data_source
        ,row_number() over (
	        partition by patient_id
	        order by case when enrollment_end_date is null
                then cast ('2050-01-01' as date)
                else enrollment_end_date end DESC)
            as row_sequence
    from {{ ref('input_layer__eligibility')}}
)

select
    cast(patient_id as {{ dbt.type_string() }}) as patient_id
    ,cast(gender as {{ dbt.type_string() }}) as gender
    ,cast(race as {{ dbt.type_string() }}) as race
    ,cast(birth_date as date) as birth_date
    ,cast(death_date as date) as death_date
    ,cast(death_flag as int) as death_flag
    ,cast(first_name as {{ dbt.type_string() }}) as first_name
    ,cast(last_name as {{ dbt.type_string() }}) as last_name
    ,cast(address as {{ dbt.type_string() }}) as address
    ,cast(city as {{ dbt.type_string() }}) as city
    ,cast(state as {{ dbt.type_string() }}) as state
    ,cast(zip_code as {{ dbt.type_string() }}) as zip_code
    ,cast(phone as {{ dbt.type_string() }}) as phone
    ,cast(data_source as {{ dbt.type_string() }}) as data_source
from patient_stage
where row_sequence = 1
