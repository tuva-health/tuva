{{ config(
     enabled = var('acute_inpatient_enabled',var('tuva_marts_enabled',True))
   )
}}

select
  patient_id
, birth_date
, gender
, race
from {{ ref('eligibility') }}