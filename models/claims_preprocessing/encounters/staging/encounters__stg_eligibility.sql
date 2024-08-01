select
    patient_id
    , birth_date
    , gender
    , race
    , row_number() over (partition by patient_id order by enrollment_start_date desc) patient_row_num
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__eligibility') }}
