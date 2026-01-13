{{ config(
     enabled = (  var('hedis_measures_enabled', False) == True  and
                  var('claims_enabled', var('clinical_enabled', False))
               ) | as_bool
   )
}}

select
  person_id
, enrollment_start_date
, enrollment_end_date
from {{ ref('core__eligibility') }}
where living_long_term_in_institution = 1