select
    patient_sk
    , data_source
    , member_id
    , first_name
    , middle_name
    , last_name
    , suffix
    , gender
    , race
    , birth_date
    , death_date
    , death_flag
    , social_security_number
    , address
    , city
    , state
    , zip_code
    , phone
    , email
    , {{ current_timestamp() }} as tuva_last_run
from {{ ref('the_tuva_project', 'core__stg_claims_patient') }}