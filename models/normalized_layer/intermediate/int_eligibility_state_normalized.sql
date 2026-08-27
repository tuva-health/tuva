{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}


select distinct
    elig.person_id
    , elig.person_id_key
    , ansi.ansi_fips_state_name as normalized_state_name
    , ansi.ansi_fips_state_code as fips_state_code
    , ansi.ansi_fips_state_abbreviation as fips_state_abbreviation
from {{ ref('int_eligibility_casting') }} as elig
left outer join {{ ref('terminology__ansi_fips_state') }} as ansi
  on (
       trim(lower(elig.state)) = trim(lower(ansi.ansi_fips_state_abbreviation))
    or trim(lower(elig.state)) = trim(lower(ansi.ansi_fips_state_code))
    or trim(lower(elig.state)) = trim(lower(ansi.ansi_fips_state_name))
  )
