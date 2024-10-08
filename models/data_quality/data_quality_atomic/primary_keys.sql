{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with eligibility_pk_prep as ( 
/*Eligibility PK test*/
select 
    patient_id 
    , member_id
    , enrollment_start_date
    , enrollment_end_date 
    , count(*) as duplicate_eligibility
from 
    {{ref('eligibility')}}
group by 
    patient_id
    , member_id 
    , enrollment_start_date 
    , enrollment_end_date
) 

, eligibility_pk as (
select 
    patient_id 
    , member_id
    , enrollment_start_date
    , enrollment_end_date 
    , duplicate_eligibility 
from 
    eligibility_pk_prep
where 
    duplicate_eligibility > 1 
) 

, medical_claim_expected_pk as (
select 
    claim_id 
    , MAX(claim_line_number) as expected_pks
from 
    {{ref('medical_claim')}}
group by 
    claim_id 
)

, medical_claim_pk_prep as (
/*Medical Claim PK test*/
select 
    claim_id
    , claim_line_number
    , data_source
    , count(*) as duplicate_count
from 
    {{ref('medical_claim')}}
group by 
    claim_id
    , claim_line_number
    , data_source
) 

, medical_claim_final as (
select 
    medical_claim_pk_prep.claim_id
    , case when sum(medical_claim_pk_prep.duplicate_count) > medical_claim_expected_pk.expected_pks then 1 else 0 end as duplicate_medical_claim_pks
from 
    medical_claim_pk_prep 
    inner join 
    medical_claim_expected_pk 
    on medical_claim_pk_prep.claim_id = medical_claim_expected_pk.claim_id 
group by 
    medical_claim_pk_prep.claim_id 
    , medical_claim_expected_pk.expected_pks
)


, pharmacy_claim_expected_pk as (
select 
    claim_id 
    , MAX(claim_line_number) as expected_pks
from 
    {{ref('pharmacy_claim')}} 
group by 
    claim_id 
)

, pharmacy_claim_pk_prep as (
/*Medical Claim PK test*/
select 
    claim_id
    , claim_line_number
    , data_source
    , count(*) as duplicate_count
from 
    {{ref('pharmacy_claim')}} 
group by 
    claim_id
    , claim_line_number
    , data_source
)

, pharmacy_claim_final as (
select 
    pharmacy_claim_pk_prep.claim_id
    , case when sum(pharmacy_claim_pk_prep.duplicate_count) > pharmacy_claim_expected_pk.expected_pks then 1 else 0 end as duplicate_pharmacy_claim_pks
from 
    pharmacy_claim_pk_prep 
    inner join 
    pharmacy_claim_expected_pk 
    on pharmacy_claim_pk_prep.claim_id = pharmacy_claim_expected_pk.claim_id 
group by 
    pharmacy_claim_pk_prep.claim_id 
    , pharmacy_claim_expected_pk.expected_pks
)

, summary as (
-- Final select to handle each case, including when no rows are returned
select 
    'eligibility' as table_name
    , coalesce(sum(duplicate_eligibility), 0) as duplicate_pk,
from 
    eligibility_pk

union all

select 
     'medical claim' as table_name
    , coalesce(sum(duplicate_medical_claim_pks), 0) as duplicate_pk,
from 
    medical_claim_final

union all

select 
    'pharmacy claim' as table_name
    , coalesce(sum(duplicate_pharmacy_claim_pks), 0) as duplicate_pk
from 
    pharmacy_claim_final
) 

select  
    table_name 
    , duplicate_pk
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from  
summary 
