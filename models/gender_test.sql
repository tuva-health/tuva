{{ config(materialized='view') }}

select 
    patient_id
,   'invalid_gender_code' as test_name
,   1 as test_flag
from {{ ref('patients') }} a
left join {{ ref('gender_codes') }} b
    on a.gender_code = b.code
where a.gender_code is not null
    and b.code is null