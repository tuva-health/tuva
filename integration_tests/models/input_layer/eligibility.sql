select 
    cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(member_id as {{ dbt.type_string() }}) as member_id
    , cast(subscriber_id as {{ dbt.type_string() }}) as subscriber_id
    , cast(gender as {{ dbt.type_string() }}) as gender
    , cast(race as {{ dbt.type_string() }}) as race
    , cast(birth_date as {{ dbt.type_timestamp() }}) as birth_date
    , cast(death_date as {{ dbt.type_timestamp() }}) as death_date
    , cast(death_flag as {{ dbt.type_boolean() }}) as death_flag
    , cast(enrollment_start_date as date) as enrollment_start_date
    , cast(enrollment_end_date as date) as enrollment_end_date
    , cast(payer as {{ dbt.type_string() }}) as payer
    , cast(payer_type as {{ dbt.type_string() }}) as payer_type
    , cast(plan as {{ dbt.type_string() }}) as plan
    , cast(original_reason_entitlement_code as {{ dbt.type_string() }}) as original_reason_entitlement_code
    , cast(dual_status_code as {{ dbt.type_string() }}) as dual_status_code
    , cast(medicare_status_code as {{ dbt.type_string() }}) as medicare_status_code
    , cast(group_id as {{ dbt.type_string() }}) as group_id
    , cast(group_name as {{ dbt.type_string() }}) as group_name
    , cast(first_name as {{ dbt.type_string() }}) as first_name
    , cast(last_name as {{ dbt.type_string() }}) as last_name
    , cast(social_security_number as {{ dbt.type_string() }}) as social_security_number
    , cast(subscriber_relation as {{ dbt.type_string() }}) as subscriber_relation
    , cast(address as {{ dbt.type_string() }}) as address
    , cast(city as {{ dbt.type_string() }}) as city
    , cast(state as {{ dbt.type_string() }}) as state
    , cast(zip_code as {{ dbt.type_string() }}) as zip_code
    , cast(phone as {{ dbt.type_string() }}) as phone
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(file_name as {{ dbt.type_string() }}) as file_name
    , cast(file_date as date) as file_date
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
from {{ ref('tuva_data_assets', 'eligibility') }}