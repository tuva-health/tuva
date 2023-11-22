{{ config(
     enabled = var('financial_pmpm_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
  patient_id
, enrollment_start_date
, enrollment_end_date
, payer
, plan
, data_source
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__eligibility') }} 
