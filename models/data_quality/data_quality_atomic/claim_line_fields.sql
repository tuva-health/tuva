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
    , case when hcpcs_code is null then 0 when strlen(hcpcs_code) != 5 then 1 else 0 end as hcpcs_code_invalid 
    {% else %}
    , case when revenue_center_code is null then 0 when length(revenue_center_code) != 2 then 1 else 0 end as revenue_center_code_invalid 
    , case when place_of_service_code is null then 0 when length(place_of_service_code) != 2 then 1 else 0 end as place_of_service_code_invalid
    , case when hcpcs_code is null then 0 when length(hcpcs_code) != 5 then 1 else 0 end as hcpcs_code_invalid 
    , case when revenue_center_code is null then 1 else 0 end as revenue_center_code_missing
    , case when place_of_service_code is null then 1 else 0 end as place_of_service_code_missing 
    , case when hcpcs_code is null then 1 else 0 end as hcpcs_code_missing
from 
    {{ref('medical_claim')}}
) 

, medical_claim_missing_or_invald_prep AS (
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

, medical_claim_missing_or_invalid as (
select 
claim_id
, case when revenue_center_code_missing > 0 then 1 else 0 end as revenue_center_code_missing
, case when place_of_service_code_missing > 0 then 1 else 0 end as place_of_service_code_missing
, case when hcpcs_code_missing > 0 then 1 else 0 end as hcpcs_code_missing
, case when revenue_center_code_invalid > 0 then 1 else 0 end as revenue_center_code_invalid
, case when place_of_service_code_invalid > 0 then 1 else 0 end as place_of_service_code_invalid
, case when hcpcs_code_invalid > 0 then 1 else 0 end as hcpcs_code_invalid
from 
medical_claim_missing_or_invald_prep
)

, final as (
select 
    coalesce(sum(revenue_center_code_missing), 0) AS result_count
    , 'medical claim revenue_center_code missing' AS data_quality_check
from 
    medical_claim_missing_or_invalid

union all  

select 
    coalesce(sum(place_of_service_code_missing), 0) AS result_count
    , 'medical claim place_of_service code missing' AS data_quality_check
from 
    medical_claim_missing_or_invalid

union all 

select 
    coalesce(sum(hcpcs_code_missing), 0) AS result_count
    , 'medical claim hcpcs_code missing' AS data_quality_check
from 
    medical_claim_missing_or_invalid

union all 

select 
    coalesce(sum(revenue_center_code_invalid), 0) AS result_count
    , 'medical claim revenue_center_code invalid' AS data_quality_check
from 
    medical_claim_missing_or_invalid

union all 

select 
    coalesce(sum(place_of_service_code_invalid), 0) AS result_count
    , 'medical claim place_of_service code invalid' AS data_quality_check
from 
    medical_claim_missing_or_invalid

union all 

select 
    coalesce(sum(hcpcs_code_invalid), 0) AS result_count
    , 'medical claim hcpcs_code invalid' AS data_quality_check
from 
    medical_claim_missing_or_invalid
) 

select 
    data_quality_check
    , result_count 
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from 
    final 