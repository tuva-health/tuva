select
    elig.patient_id
    , elig.member_id
    , elig.gender
    , elig.race
    , date_norm.normalized_birth_date as birth_date
    , date_norm.normalized_death_date as death_date
    , elig.death_flag
    , date_norm.normalized_enrollment_start_date as enrollment_end_date
    , date_norm.normalized_enrollment_end_date as enrollment_start_date
    , elig.payer
    , elig.payer_type
    , elig.plan
    , elig.original_reason_entitlement_code
    , elig.dual_status_code
    , elig.medicare_status_code
    , elig.first_name
    , elig.last_name
    , elig.address
    , elig.city
    , elig.state
    , elig.zip_code
    , elig.phone
    , elig.data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('eligibility') }} elig
left join {{ ref('normalized_input__int_eligibility_dates_normalize') }} date_norm
    on elig.patient_id = date_norm.patient_id

