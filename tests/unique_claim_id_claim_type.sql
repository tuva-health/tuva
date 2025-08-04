{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool,
     tags = ['dqi', 'tuva_dqi_sev_2', 'dqi_service_categories', 'dqi_ccsr', 'dqi_cms_chronic_conditions',
            'dqi_tuva_chronic_conditions', 'dqi_cms_hccs', 'dqi_ed_classification',
            'dqi_financial_pmpm', 'dqi_quality_measures', 'dqi_readmission'],
   )
}}

-- This test ensures only 1 claim type per claim
with distinct_combinations as (
  select distinct
    claim_id,
    claim_type
  from {{ ref('medical_claim') }}
),

duplicates as (
  select
    claim_id,
    count(*) as n_records
  from distinct_combinations
  group by claim_id
  having count(*) > 1
)

select * from duplicates