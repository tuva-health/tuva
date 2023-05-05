{{ config(
     enabled = var('data_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

select * from {{ ref('data_profiling__test_result_stage_medical_claim') }}

union all

select * from {{ ref('data_profiling__test_result_stage_eligibility') }}

union all

select * from {{ ref('data_profiling__test_result_stage_pharmacy_claim') }}
