{{ config(
     enabled = var('claims_enabled', False)
 | as_bool
   )
}}


select distinct
    elig.person_id
  , elig.person_id_key
  , cal_dob.full_date as normalized_birth_date
  , cal_death.full_date as normalized_death_date
  , cal_enroll_start.full_date as normalized_enrollment_start_date
  , cal_enroll_end.full_date as normalized_enrollment_end_date
from {{ ref('int_eligibility_casting') }} as elig
left outer join {{ ref('reference_data__calendar') }} as cal_dob
    on elig.birth_date = cal_dob.full_date
left outer join {{ ref('reference_data__calendar') }} as cal_death
    on elig.death_date = cal_death.full_date
left outer join {{ ref('reference_data__calendar') }} as cal_enroll_start
    on elig.enrollment_start_date = cal_enroll_start.full_date
left outer join {{ ref('reference_data__calendar') }} as cal_enroll_end
    on elig.enrollment_end_date = cal_enroll_end.full_date
