{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select distinct
  elig.patient_id
  , elig.patient_id||elig.data_source||elig.payer||elig.plan||elig.enrollment_start_date||elig.enrollment_end_date as patient_id_key
  , cal_dob.full_date as normalized_birth_date
  , cal_death.full_date as normalized_death_date
  , cal_enroll_start.full_date as normalized_enrollment_start_date
  , cal_enroll_end.full_date as normalized_enrollment_end_date
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__stg_eligibility') }} elig
left join {{ ref('terminology__calendar') }} cal_dob
    on elig.birth_date = cal_dob.full_date
left join {{ ref('terminology__calendar') }} cal_death
    on elig.death_date = cal_death.full_date
left join {{ ref('terminology__calendar') }} cal_enroll_start
    on elig.enrollment_start_date = cal_enroll_start.full_date
left join {{ ref('terminology__calendar') }} cal_enroll_end
    on elig.enrollment_end_date = cal_enroll_end.full_date
