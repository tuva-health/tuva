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
, payer
, payer_type
, institutional_special_needs_plan --need to add to this table
from {{ ref('core__eligibility') }}