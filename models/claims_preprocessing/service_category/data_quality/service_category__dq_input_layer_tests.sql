{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select distinct 
  claim_id
, 'Multiple claim_type' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
group by 1
having count(distinct claim_type) > 1

union all

select distinct 
  claim_id
, 'Multiple bill_type_code' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
group by 1
having count(distinct bill_type_code) > 1

union all

select distinct 
  claim_id
, 'Missing claim_type' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type is null

union all

select distinct 
  claim_id
, 'Missing place_of_service_code' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'professional'
  and place_of_service_code is null

union all

select distinct 
  claim_id
, 'Missing bill_type_code' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'institutional'
  and bill_type_code is null

union all

select distinct 
  claim_id
, 'Missing revenue_center_code' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'institutional'
  and revenue_center_code is null

union all

select distinct 
  claim_id
, 'Missing hcpcs_code' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'professional'
  and hcpcs_code is null

union all

select distinct 
  claim_id
, 'Invalid claim_type' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} a
inner join {{ ref('terminology__claim_type') }} b
  on a.claim_type = b.claim_type
where b.claim_type is null

union all

select distinct 
  claim_id
, 'Invalid place_of_service_code' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} a
inner join {{ ref('terminology__place_of_service') }} b
  on a.place_of_service_code = b.place_of_service_code
where a.claim_type = 'professional'
  and b.place_of_service_code is null

union all

select distinct 
  claim_id
, 'Invalid bill_type_code' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} a
inner join {{ ref('terminology__bill_type') }} b
  on a.bill_type_code = b.bill_type_code
where a.claim_type = 'institutional'
  and b.bill_type_code is null

union all

select distinct 
  claim_id
, 'Invalid revenue_center_code' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} a
inner join {{ ref('terminology__revenue_center') }} b
  on a.revenue_center_code = b.revenue_center_code
where a.claim_type = 'institutional'
  and b.revenue_center_code is null

union all

select distinct 
  claim_id
, 'Invalid hcpcs_code' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} a
inner join {{ ref('terminology__hcpcs_level_2') }} b
  on a.hcpcs_code = b.hcpcs
where a.claim_type = 'professional'
  and b.hcpcs is null

union all

select distinct 
  claim_id
, 'Invalid ms_drg_code' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} a
inner join {{ ref('terminology__ms_drg') }} b
  on a.ms_drg_code = b.ms_drg_code
where a.claim_type = 'institutional'
  and b.ms_drg_code is null

union all

select distinct 
  claim_id
, 'Invalid apr_drg_code' as dq_problem
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} a
inner join {{ ref('terminology__apr_drg') }} b
  on a.apr_drg_code = b.apr_drg_code
where a.claim_type = 'institutional'
  and b.apr_drg_code is null