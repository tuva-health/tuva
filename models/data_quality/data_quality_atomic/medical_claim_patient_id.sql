{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with medical_claim_patient_id_null as (
select 
    claim_id 
    , case when patient_id is null then 1 else 0 end as patient_id_null
from 
    {{ref('core__medical_claim')}}
) 

, medical_claim_patient_id_null_sum as (
select 
    claim_id
    , case when sum(patient_id_null) > 0 then 1 else 0 end as patient_id_null_sum 
from 
    medical_claim_patient_id_null
group by 
    claim_id 
)

, medical_claim_multiple_patient_ids_prep as (
select 
    claim_id
    , count(distinct patient_id) as number_of_patient_ids
from 
    {{ref('core__medical_claim')}}
group by claim_id
) 

, medical_claim_multiple_patient_ids as (
select 
    claim_id 
    , case when number_of_patient_ids > 1 then 1 else 0 end as duplicate_patient_ids
from 
medical_claim_multiple_patient_ids_prep
) 

, orphaned_medical_claims as (
select  
    medical_claim.claim_id
    , medical_claim.member_id as claim_member_ID 
    , eligibility.member_ID as eligibility_member_ID 
    , case when eligibility.member_id is null then 1 else 0 end as orphaned_claim
from  
    {{ref('core__medical_claim')}}
left join
    {{ref('core__eligibility')}}
    on medical_claim.member_id = eligibility.member_id
) 

, orphaned_medical_claims_final as (
select 
    claim_id
    , SUM(orphaned_claim) AS orphaned_claim
from 
    orphaned_medical_claims
group by 
    claim_id
)

, summary AS (
select  
    'multiple patient ids' AS data_quality_check
    , coalesce(sum(duplicate_patient_ids),0) AS result
from 
    medical_claim_multiple_patient_ids

union all

select 
    'missing patient ids' AS data_quality_check
    , coalesce(sum(patient_id_null_sum),0) AS result
from 
    medical_claim_patient_id_null_sum

union all

select 
    'orphaned claims' AS data_quality_check
    , coalesce(sum(orphaned_claim),0) AS result
from 
    orphaned_medical_claims_final
) 

select  
    data_quality_check 
    , result 
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from  
    summary 