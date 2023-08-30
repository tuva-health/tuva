{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with stage as(
  select distinct
        claim_id
        , claim_type
        , patient_id
        , member_id
        , claim_start_date
        , claim_end_date
        , paid_date
        , diagnosis_code_type
        , diagnosis_code_1
        , diagnosis_code_2
        , diagnosis_code_3
        , diagnosis_code_4
        , diagnosis_code_5
        , diagnosis_code_6
        , diagnosis_code_7
        , diagnosis_code_8
        , diagnosis_code_9
        , diagnosis_code_10
        , diagnosis_code_11
        , diagnosis_code_12
        , diagnosis_code_13
        , diagnosis_code_14
        , diagnosis_code_15
        , diagnosis_code_16
        , diagnosis_code_17
        , diagnosis_code_18
        , diagnosis_code_19
        , diagnosis_code_20
        , diagnosis_code_21
        , diagnosis_code_22
        , diagnosis_code_23
        , diagnosis_code_24
        , diagnosis_code_25
        , data_source
  from {{ ref('medical_claim') }} med
  where claim_type = 'professional'
  )  
, claims_with_duplicates as (
select 
      claim_id
    , 1 as invalid_header
from stage
group by claim_id
having count(*) > 1
)

select med.*, '{{ var('tuva_last_run')}}' as tuva_last_run from claims_with_duplicates dupe
inner join {{ ref('medical_claim') }} med
    on dupe.claim_id = med.claim_id