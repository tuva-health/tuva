select distinct
    surrogate_key
    , cal_dob.day_date as normalized_birth_date
    , cal_death.day_date as normalized_death_date
    , cal_enroll_start.day_date as normalized_enrollment_start_date
    , cal_enroll_end.day_date as normalized_enrollment_end_date
from {{ ref('the_tuva_project', 'normalized_input__stg_eligibility') }} as elig
    left outer join {{ ref('tuva_data_assets', 'calendar') }} as cal_dob
    on elig.birth_date = cal_dob.day_date
    left outer join {{ ref('tuva_data_assets', 'calendar') }} as cal_death
    on elig.death_date = cal_death.day_date
    left outer join {{ ref('tuva_data_assets', 'calendar') }} as cal_enroll_start
    on elig.enrollment_start_date = cal_enroll_start.day_date
    left outer join {{ ref('tuva_data_assets', 'calendar') }} as cal_enroll_end
    on elig.enrollment_end_date = cal_enroll_end.day_date