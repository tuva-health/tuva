with enrollment__patient as (
    select *
    from {{ ref('the_tuva_project', 'enrollment__patient') }}
)
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
from enrollment__patient