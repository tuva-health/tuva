
{{ config(materialized='view') }}

select
    patient_id
,   gender_code
,   race_code
,   birth_date
,   death_date
,   address
,   city
,   state
,   zip_code
from {{ var('stg_patients') }}