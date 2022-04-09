{{ config(materialized='table') }}

select
    cast(patient_id as varchar) as patient_id
,   cast(name as varchar) as name
,   cast(gender as varchar) as gender
,   cast(race as varchar) as race
,   cast(ethnicity as varchar) as ethnicity
,   cast(birth_date as date) as birth_date
,   cast(death_date as date) as death_date
,   cast(death_flag as int) as death_flag
,   cast(address as varchar) as address
,   cast(city as varchar) as city
,   cast(state as varchar) as state
,   cast(zip_code as int) as zip_code
,   cast(phone as varchar) as phone
,   cast(email as varchar) as email
,   cast(ssn as varchar) as ssn
,   cast(data_source as varchar) as data_source
from {{ var('src_patient') }}