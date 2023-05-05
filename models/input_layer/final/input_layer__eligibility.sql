{{ config(
     enabled = var('input_layer_enabled',var('tuva_marts_enabled',True))
   )
}}

select
         cast(patient_id as {{ dbt.type_string() }} ) as patient_id
       , cast(member_id as {{ dbt.type_string() }} ) as member_id
       , cast(gender as {{ dbt.type_string() }} ) as gender
       , cast(race as {{ dbt.type_string() }} ) as race
       , cast(birth_date as date) as birth_date
       , cast(death_date as date) as death_date
       , cast(death_flag as integer ) as death_flag
       , cast(enrollment_start_date as date ) as enrollment_start_date
       , cast(enrollment_end_date as date ) as enrollment_end_date
       , cast(payer as {{ dbt.type_string() }} ) as payer
       , cast(payer_type as {{ dbt.type_string() }} ) as payer_type
       , cast(dual_status_code as {{ dbt.type_string() }} ) as dual_status_code
       , cast(medicare_status_code as {{ dbt.type_string() }} ) as medicare_status_code
       , cast(first_name as {{ dbt.type_string() }} ) as first_name
       , cast(last_name as {{ dbt.type_string() }} ) as last_name
       , cast(address as {{ dbt.type_string() }} ) as address
       , cast(city as {{ dbt.type_string() }} ) as city
       , cast(state as {{ dbt.type_string() }} ) as state
       , cast(zip_code as {{ dbt.type_string() }} ) as zip_code
       , cast(phone as {{ dbt.type_string() }} ) as phone
       , cast(data_source as {{ dbt.type_string() }} ) as data_source
from {{ ref('eligibility')}}


