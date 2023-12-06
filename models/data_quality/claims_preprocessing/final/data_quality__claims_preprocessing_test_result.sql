{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select * from {{ ref('data_quality__claims_preprocessing_test_result_stage_medical_claim') }}

union all

select * from {{ ref('data_quality__claims_preprocessing_test_result_stage_eligibility') }}

union all

select * from {{ ref('data_quality__claims_preprocessing_test_result_stage_pharmacy_claim') }}
