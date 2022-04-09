{{ config(materialized='table') }}

select
    cast(encounter_id as varchar) as encounter_id
,   cast(patient_id as varchar) as patient_id
,   cast(component_name as varchar) as component_name
,   cast(observation_date as date) as observation_date
,   cast(value as varchar) as value
,   cast(reference_range as varchar) as reference_range
,   cast(body_site as varchar) as body_site
,   cast(specimen as varchar) as specimen
,   cast(data_source as varchar) as data_source
from {{ var('src_observation') }}