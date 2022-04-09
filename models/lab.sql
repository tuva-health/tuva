{{ config(materialized='table') }}

select
    cast(encounter_id as varchar) as encounter_id
,   cast(patient_id as varchar) as patient_id
,   cast(order_id as varchar) as order_id
,   cast(order_date as date) as order_date
,   cast(result_date as date) as result_date
,   cast(component_name as varchar) as component_name
,   cast(loinc as varchar) as loinc
,   cast(loinc_description as varchar) as loinc_description
,   cast(result as varchar) as result
,   cast(units as varchar) as units
,   cast(reference_range as varchar) as reference_range
,   cast(specimen as varchar) as specimen
,   cast(data_source as varchar) as data_source
from {{ var('src_lab') }}