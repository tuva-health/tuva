{{ config(
     enabled = (  var('hedis_measures_enabled', False) == True  and
                  (var('claims_enabled', False) == True or var('clinical_enabled', False) == True)
               ) | as_bool
   )
}}

select
  person_id
, enrollment_start_date
, enrollment_end_date
, payer
, payer_type
, institutional_snp_flag as institutional_special_needs_plan
from {{ ref('core__eligibility') }}
