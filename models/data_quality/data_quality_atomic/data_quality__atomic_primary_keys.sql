{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with eligibility_pk as ( 
/*Eligibility PK test*/
select 
    patient_id 
    , member_id
    , enrollment_start_date
    , enrollment_end_date 
    , count(*) as duplicate_count
from 
    {{ref('eligibility')}}
group by 
    patient_id
    , member_id 
    , enrollment_start_date 
    , enrollment_end_date
having 
    count(*) > 1
) 

, medical_claim_pk as (
/*Medical Claim PK test*/
select 
    claim_id
    , claim_line_number
    , count(*) as duplicate_count
from 
    {{ref('medical_claim')}}
group by 
    claim_id
    , claim_line_number
having 
    count(*) > 1
) 

, medical_claim_final as (
select 
    claim_id
    , SUM(duplicate_count) AS duplicate_count
from 
    medical_claim_pk 
group by 
    claim_id 
)

, Pharmacy_Claim_PK as (
/*Pharmacy Claim PK test*/
select 
    claim_id
    , claim_line_number
    , count(*) AS duplicate_count
from 
    {{ref('pharmacy_claim')}}
group by 
    claim_id
    , claim_line_number
having 
    count(*) > 1
) 

, pharmacy_claim_final as (
select 
    claim_id
    , SUM(duplicate_count) as duplicate_count
from 
    pharmacy_claim_pk
group by 
    claim_id 
)

, summary as (
-- Final select to handle each case, including when no rows are returned
select 
    'primary_key_duplicate' as test_name
    , 'eligibility' as test_source
    , coalesce(sum(duplicate_count), 0) as flagged_records,
from 
    eligibility_pk

union all

select 
    'primary_key_duplicate' as test_name
    , 'medical_claim' as test_source
    , coalesce(sum(duplicate_count), 0) as flagged_records,
from 
    medical_claim_final

union all

select 
    'primary_key_duplicate' as test_name
    , 'pharmacy_claim' as test_source
    , coalesce(sum(duplicate_count), 0) as flagged_records
from 
    pharmacy_claim_final
) 

select  
    test_name
    , test_source 
    , flagged_records
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from  
summary 
