{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with drg_requirement as (
select distinct 
  mc.claim_id
from {{ ref('service_category__stg_medical_claim') }} mc
left join {{ ref('terminology__ms_drg')}} msdrg
  on mc.ms_drg_code = msdrg.ms_drg_code
left join {{ ref('terminology__apr_drg')}} aprdrg
  on mc.apr_drg_code = aprdrg.apr_drg_code
where claim_type = 'institutional'
  and (msdrg.ms_drg_code is not null or aprdrg.apr_drg_code is not null)
)

, bill_type_requirement as (
select distinct 
  claim_id
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'institutional'
  and substring(bill_type_code, 1, 2) in ('11','12')
)

select distinct 
  a.claim_id
, 'Acute Inpatient - Other' as service_category_2
, 'Acute Inpatient - Other' as service_category_3
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} a
inner join bill_type_requirement d
  on a.claim_id = d.claim_id

union distinct 

select distinct 
  a.claim_id
, 'Acute Inpatient - Other' as service_category_2
, 'Acute Inpatient - Other' as service_category_3
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} a
inner join drg_requirement c
  on a.claim_id = c.claim_id
