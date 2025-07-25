select
    elig.eligibility_sk
    , elig.data_source
    , elig.person_id
    , elig.member_id
    , elig.subscriber_id
    , elig.gender
    , elig.race
    , date_norm.normalized_birth_date as birth_date
    , date_norm.normalized_death_date as death_date
    , elig.death_flag
    , date_norm.normalized_enrollment_start_date as enrollment_start_date
    , date_norm.normalized_enrollment_end_date as enrollment_end_date
    , elig.payer
    , elig.payer_type
    , elig.{{ quote_column('plan') }}
    , elig.original_reason_entitlement_code
    , elig.dual_status_code
    , elig.medicare_status_code
    , elig.group_id
    , elig.group_name
    , elig.first_name
    , elig.last_name
    , elig.social_security_number
    , elig.subscriber_relation
    , elig.address
    , elig.city
    , elig.state
    , elig.zip_code
    , elig.phone
    , elig.file_name
    , elig.file_date
    , elig.ingest_datetime
from {{ ref('the_tuva_project', 'normalized_input__stg_eligibility') }} as elig
left outer join {{ ref('the_tuva_project', 'normalized_input__int_eligibility_dates_normalize') }} as date_norm
    on elig.eligibility_sk = date_norm.eligibility_sk