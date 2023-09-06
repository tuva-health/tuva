{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with claim_start_date_after_claim_end_date as (
  select
      'claim start date after claim end date' as test_name
      , 'medical_claim' as source_table
      , 'all' as claim_type
      , 'plausibility' as test_category
      , 'claim_id' as grain
      , claim_id
      , count(*) as counts
      , '{{ var('tuva_last_run')}}' as tuva_last_run
  from {{ ref('medical_claim') }}
  where claim_start_date > claim_end_date
  group by
      claim_id
)
, admission_date_after_discharge_date as (
  select
      'admission date after discharge date' as test_name
      , 'medical_claim' as source_table
      , 'institutional' as claim_type
      , 'plausibility' as test_category
      , 'claim_id' as grain
      , claim_id
      , count(*) as counts
      , '{{ var('tuva_last_run')}}' as tuva_last_run
  from {{ ref('medical_claim') }}
  where claim_type = 'institutional'
  and admission_date > discharge_date
  group by
      claim_id
)

select * from claim_start_date_after_claim_end_date
union all
select * from admission_date_after_discharge_date