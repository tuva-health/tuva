{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with missing_medical_claim_fields as (
select 
    claim_id 
    , claim_line_number
    , case when revenue_center_code is null then 1 else 0 end as revenue_center_code_missing
    , case when place_of_service_code is null then 1 else 0 end as place_of_service_code_missing 
    , case when hcpcs_code is null then 1 else 0 end as hcpcs_code_missing
from 
    {{ref('core__medical_claim')}}
) 

, medical_claim_missing AS (
select 
    claim_id
    , sum(revenue_center_code_missing) AS revenue_center_code_missing 
    , sum(place_of_service_code_missing) AS place_of_service_code_missing
    , sum(hcpcs_code_missing) AS hcpcs_code_missing 
from
    missing_medical_claim_fields
group by 
    claim_id 
)

, final as (
select 
    coalesce(sum(revenue_center_code_missing), 0) AS flagged_records
    , 'medical_claim_revenue_center_code_missing' AS test_name
    , 'medical_claim' AS test_source 
from 
    medical_claim_missing

union all  

select 
    coalesce(sum(place_of_service_code_missing), 0) AS flagged_records
    , 'medical_claim_place_of_service_code_missing' AS test_name
    , 'medical_claim' AS test_source 
from 
    medical_claim_missing

union all 

select 
    coalesce(sum(hcpcs_code_missing), 0) AS flagged_records
    , 'medical_claim_hcpcs_code_missing' AS test_name
    , 'medical_claim' AS test_source 
from 
    medical_claim_missing
) 

select 
    test_name 
    , test_source
    , flagged_records
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from 
    final 