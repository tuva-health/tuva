{{ config(materialized='table') }}

select
    cast(encounter_id as varchar) as encounter_id
,   cast(patient_id as varchar) as patient_id
,   cast(procedure_date as date) as procedure_date
,   cast(code_type as varchar) as code_type
,   cast(code as varchar) as code
,   cast(description as varchar) as description
,   cast(physician_npi as varchar) as physician_npi
,   cast(data_source as varchar) as data_source
from {{ var('src_procedure') }}
