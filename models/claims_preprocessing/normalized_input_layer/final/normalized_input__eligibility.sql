select
    elig.patient_id
    , elig.member_id
    , elig.gender
    , elig.race
    , date_norm.birth_date
    , date_norm.death_date
    , elig.death_flag
    , date_norm.enrollment_start_date
    , date_norm.enrollment_end_date
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
from {{ ref('eligibility') }} elig
left join {{ ref('normalized_input__int_eligibility_dates_normalize') }} date_norm
    on elig.patient_id = date_norm.patient_id

