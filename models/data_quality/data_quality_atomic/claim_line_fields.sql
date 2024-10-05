{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with missing_medical_claim_fields as (
select 
    claim_id 
    , claim_line_number
    {% if target.type == 'fabric' %}
    , case when revenue_center_code is null then 0 when strlen(revenue_center_code) != 2 then 1 else 0 end as revenue_center_code_invalid 
    , case when place_of_service_code is null then 0 when strlen(place_of_service_code) != 2 then 1 else 0 end as place_of_service_code_invalid
    , case when is null then 0 when strlen(hcpcs_code) != 5 then 1 else 0 end as hcpcs_code_invalid 
    {% else %}
    , case when revenue_center_code is null then 0 when length(revenue_center_code) != 2 then 1 else 0 end as revenue_center_code_invalid 
    , case when place_of_service_code is null then 0 when length(place_of_service_code) != 2 then 1 else 0 end as place_of_service_code_invalid
    , case when is null then 0 when length(hcpcs_code) != 5 then 1 else 0 end as hcpcs_code_invalid 
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
    , sum(revenue_center_code_invalid) AS revenue_center_code_invalid 
    , sum(place_of_service_code_invalid) AS place_of_service_code_invalid
    , sum(hcpcs_code_invalid) AS hcpcs_code_invalid  
from
    missing_medical_claim_fields
group by 
    claim_id 
)

, final as (
select 
    coalesce(sum(revenue_center_code_missing), 0) AS result
    , 'revenue center code missing' AS data_quality_check
from 
    medical_claim_missing

union all  

select 
    coalesce(sum(place_of_service_code_missing), 0) AS result
    , 'place of service code missing' AS data_quality_check
from 
    medical_claim_missing

union all 

select 
    coalesce(sum(hcpcs_code_missing), 0) AS result
    , 'medical claim hcpcs code missing' AS data_quality_check
from 
    medical_claim_missing

union all 

select 
    coalesce(sum(revenue_center_code_invalid), 0) AS result
    , 'medical claim revenue center code invalid' AS data_quality_check
from 
    medical_claim_missing

union all 

select 
    coalesce(sum(place_of_service_code_invalid), 0) AS result
    , 'medical claim place of service code invalid' AS data_quality_check
from 
    medical_claim_missing

union all 

select 
    coalesce(sum(hcpcs_code_invalid), 0) AS result
    , 'medical claim hcpcs code invalid' AS data_quality_check
from 
    medical_claim_missing

) 

select 
    data_quality_check
    , result 
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from 
    final 