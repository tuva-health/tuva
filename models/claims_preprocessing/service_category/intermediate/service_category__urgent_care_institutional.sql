{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with multiple_sources as (
select distinct
  a.claim_id
, 'urgent care' as service_category_2
, 'urgent care' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as a
where claim_type = 'institutional'
  and revenue_center_code = '0456'
  and substring(bill_type_code, 1, 2) in ('13', '71', '73')

union all

select distinct
  a.claim_id
, 'urgent care' as service_category_2
, 'urgent care' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as a
where claim_type = 'institutional'
  and hcpcs_code in ('S9088', '99051', 'S9083')
)

select distinct
claim_id
, 'outpatient' as service_category_1
, service_category_2
, service_category_3
, source_model_name
, tuva_last_run
from multiple_sources
