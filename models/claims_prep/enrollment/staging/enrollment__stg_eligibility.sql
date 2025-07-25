with normalized_input__eligibility as (
    select *
    from {{ ref('normalized_input__eligibility') }}
)
select
    eligibility_sk
    , data_source
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , enrollment_start_date
    , enrollment_end_date
    , first_name
    , last_name
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
from normalized_input__eligibility