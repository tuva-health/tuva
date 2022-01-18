{{ config(materialized='view') }}

with claims_with_dx as (
select distinct claim_id
from {{ ref('diagnoses') }} 
where diagnosis_code is not null
)

, claims_without_dx as (
select distinct claim_id
from {{ ref('diagnoses') }}
where diagnosis_code is null
)

select distinct 
    a.claim_id
,   'missing_diagnosis_code' as test_name
,   1 as test_flag
from claims_without_dx a
left join claims_with_dx b
    on a.claim_id = b.claim_id
where b.claim_id is null