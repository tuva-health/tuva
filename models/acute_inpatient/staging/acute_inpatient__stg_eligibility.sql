{{ config(
     enabled = var('acute_inpatient_enabled',var('tuva_marts_enabled',True))
   )
}}

select
  patient_id
, birth_date
, gender
, race
, '{{ var('last_update')}}' as last_update
from {{ ref('eligibility') }}