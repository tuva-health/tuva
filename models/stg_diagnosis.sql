{{ config(materialized='table', tags='core') }}

select
    encounter_id
,   code_type
,   diagnosis_code
,   diagnosis_rank
,   present_on_admission_code   
from {{ var('src_diagnosis') }}
