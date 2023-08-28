{{ config(
     enabled = var('member_months_enabled',var('tuva_marts_enabled',True))
   )
}}

select
  patient_id
, enrollment_start_date
, enrollment_end_date
, payer
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__eligibility') }} 