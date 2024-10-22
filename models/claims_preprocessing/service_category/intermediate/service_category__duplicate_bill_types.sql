{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
  claim_id
, count(distinct bill_type_code) as cnt
from {{ ref('service_category__stg_medical_claim') }}
group by claim_id
having count(distinct bill_type_code) > 1