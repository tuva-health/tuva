{{ config(materialized='table') }}

select
    cast(encounter_id as varchar) as encounter_id
,   cast(patient_id as varchar) as patient_id
,   cast(request_date as date) as request_date
,   cast(filled_date as date) as filled_date
,   cast(paid_date as date) as paid_date
,   cast(request_status as varchar) as request_status
,   cast(medication_name as varchar) as medication_name
,   cast(ndc as varchar) as ndc
,   cast(rx_norm as varchar) as rx_norm
,   cast(dose as varchar) as dose
,   cast(dose_unit as varchar) as dose_unit
,   cast(quantity as varchar) as quantity
,   cast(refills as varchar) as refills
,   cast(duration as varchar) as duration
,   cast(route as varchar) as route
,   cast(physician_npi as varchar) as physician_npi
,   cast(note as varchar) as note
,   cast(paid_amount as float) as paid_amount
,   cast(data_source as varchar) as data_source
from {{ var('src_medication') }}