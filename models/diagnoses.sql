
{{ config(materialized='view') }}

select
    encounter_id
,   diagnosis_code_type
,   diagnosis_code
,   diagnosis_rank
,   present_on_admission_code   
from {{ var('stg_diagnoses') }}
