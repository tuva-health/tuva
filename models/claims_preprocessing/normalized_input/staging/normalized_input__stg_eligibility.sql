select
    {{ dbt_utils.generate_surrogate_key(['data_source', 'member_id', 'payer', 'plan', 'enrollment_start_date', 'enrollment_end_date']) }} as eligibility_sk
    , person_id
    , member_id
    , subscriber_id
    , gender
    , race
    , birth_date
    , death_date
    , death_flag
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
    , {{ quote_column('plan') }}
    , subscriber_relation
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , group_id
    , group_name
    , first_name
    , last_name
    , social_security_number
    , address
    , city
    , state
    , zip_code
    , phone
    , data_source
    , file_name
    , file_date
    , ingest_datetime
from {{ ref('the_tuva_project', 'input_layer__eligibility') }}