{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
   )
}}

select 
  'medical claims without enrollment' as test_description
, count(distinct claim_id) as claim_count
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medical_claim') }}  a
left join {{ ref('eligibility') }}  b
    on a.patient_id = b.patient_id
where b.patient_id is null

union all

select 
  'rx claims without enrollment' as test_description
, count(distinct claim_id) as claim_count
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('pharmacy_claim') }}  a
left join {{ ref('eligibility') }}  b
    on a.patient_id = b.patient_id
where b.patient_id is null