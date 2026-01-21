{{ config(
     enabled = (  var('hedis_measures_enabled', False) == True  and
                  (var('claims_enabled', False) == True or var('clinical_enabled', False) == True)
               ) | as_bool
   )
}}

select
  person_id
, enrollment_start_date as start_date
, enrollment_end_date as end_date
from {{ ref('core__eligibility') }}
where hospice_flag = 1
