{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with multiple_sources as (
select distinct
    claim_id
from {{ ref('service_category__stg_medical_claim') }}
where place_of_service_code in ('20')

UNION

select distinct
    claim_id
from {{ ref('service_category__stg_medical_claim') }}
where hcpcs_code in ('S9088','99051','S9083')

UNION 

select distinct
    claim_id
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'institutional'
  and revenue_center_code = '0456'
  and substring(bill_type_code, 1, 2) in ('13','71','73')

)


select distinct 
claim_id
, '{{ var('tuva_last_run')}}' as tuva_last_run
from multiple_sources