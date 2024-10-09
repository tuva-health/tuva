{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

select
    'Missing patient_id' as data_quality_check
    ,count(*) as result_count
from {{ ref('eligibility')}}
where
    patient_id is null or patient_id = ''