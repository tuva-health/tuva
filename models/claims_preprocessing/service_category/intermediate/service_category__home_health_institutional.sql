{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct 
  claim_id
  , 'outpatient' as service_category_1
, 'home health' as service_category_2
, 'home health' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'institutional'
  and substring(bill_type_code, 1, 2) in (
    '31',  -- Home Health Inpatient (Part A) - Typically considered inpatient
    '32',  -- Home Health Inpatient (Part B) - Outpatient services billed by home health agencies
    '33',  -- Home Health Outpatient
    '34',  -- Home Health Other (Part B)
    '35',  -- Home Health Intermediate Care - Level I
    '36',  -- Home Health Intermediate Care - Level II
    '37',  -- Home Health Subacute Inpatient
    '38'   -- Home Health Swing Beds
  )
