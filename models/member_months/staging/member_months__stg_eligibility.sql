{{ config(
     enabled = var('member_months_enabled',var('tuva_marts_enabled',True))
   )
}}

select
  patient_id
, enrollment_start_date as start_date
, enrollment_end_date as end_date
, payer
, '{{ var('last_update')}}' as last_update
from {{ ref('eligibility') }} 
