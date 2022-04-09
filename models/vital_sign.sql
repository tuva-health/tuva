{{ config(materialized='table') }}

select
    cast(encounter_id as varchar) as encounter_id
,   cast(patient_id as varchar) as patient_id
,   cast(component_id as varchar) as component_id
,   cast(loinc as varchar) as loinc
,   cast(loinc_description as varchar) as loinc_description
,   cast(vital_date as date) as vital_date
,   cast(value as varchar) as value
,   cast(units as varchar) as units
,   cast(data_source as varchar) as data_source
from {{ var('src_vital_sign') }}