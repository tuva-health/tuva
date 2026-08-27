{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}


select distinct
    elig.person_id
  , elig.person_id_key
  , elig.birth_date as normalized_birth_date
  , elig.death_date as normalized_death_date
  , elig.enrollment_start_date as normalized_enrollment_start_date
  , elig.enrollment_end_date as normalized_enrollment_end_date
from {{ ref('int_eligibility_casting') }} as elig
