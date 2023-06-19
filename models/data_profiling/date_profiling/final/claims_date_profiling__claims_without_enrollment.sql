{{ config(
     enabled = var('claims_date_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

select 
  'medical claims without enrollment' as test_description
, count(distinct claim_id) as claim_count
, '{{ var('last_update')}}' as last_update
from {{ ref('medical_claim') }}  a
left join {{ ref('eligibility') }}  b
    on a.patient_id = b.patient_id
where b.patient_id is null

union all

select 
  'rx claims without enrollment' as test_description
, count(distinct claim_id) as claim_count
, '{{ var('last_update')}}' as last_update
from {{ ref('pharmacy_claim') }}  a
left join {{ ref('eligibility') }}  b
    on a.patient_id = b.patient_id
where b.patient_id is null