{{ config(materialized='table') }}

select
    cast(encounter_id as varchar) as encounter_id
,   cast(patient_id as varchar) as patient_id
,   cast(status as varchar) as status
,   cast(allergy_description as varchar) as allergy_description
,   cast(severity as varchar) as severity
,   cast(data_source as varchar) as data_source
from {{ var('src_allergy') }}