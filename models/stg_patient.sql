{{ config(materialized='table', tags='core') }}

select
    patient_id
,   gender
,   birth_date
,   death_date
from {{ var('src_patient') }}