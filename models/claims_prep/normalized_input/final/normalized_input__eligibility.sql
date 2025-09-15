with normalized_input__stg_eligibility as (
    select *
    from {{ ref('the_tuva_project', 'normalized_input__stg_eligibility') }}
)
select
    elig.eligibility_sk
    , elig.data_source
    , elig.person_id
    , elig.member_id
    , elig.subscriber_id
    , elig.gender
    , elig.race
    , elig.birth_date
    , elig.death_date
    , elig.death_flag
    , elig.enrollment_start_date
    , elig.enrollment_end_date
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
from normalized_input__stg_eligibility as elig