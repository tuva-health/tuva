{{ config(materialized='view') }}

select 
    claim_id
,   'invalid_diagnosis_code' as test_name
,   1 as test_flag
from {{ ref('diagnoses') }} a
left join {{ ref('icd_10_cm') }} b
    on a.diagnosis_code = b.code
where a.diagnosis_code is not null
    and b.code is null