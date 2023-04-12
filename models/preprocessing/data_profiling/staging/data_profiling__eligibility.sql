

{{ config(
     enabled = var('data_profiling_enabled',var('tuva_packages_enabled',True))
   )
}}




select
    nullif(patient_id, '') as patient_id
    , nullif(member_id, '') as member_id
    , nullif(gender, '') as gender
    , nullif(race, '') as race
    , birth_date
    , death_date
    , death_flag
    , enrollment_start_date
    , enrollment_end_date
    , nullif(payer, '') as payer
    , nullif(payer_type, '') as payer_type
    , nullif(dual_status_code, '') as dual_status_code
    , nullif(medicare_status_code, '') as medicare_status_code
    , nullif(first_name, '') as first_name
    , nullif(last_name, '') as last_name
    , nullif(address, '') as address
    , nullif(city, '') as city
    , nullif(state, '') as state
    , nullif(zip_code, '') as zip_code
    , nullif(phone, '') as phone
    , nullif(data_source, '') as data_source
from {{ var('eligibility')}}
