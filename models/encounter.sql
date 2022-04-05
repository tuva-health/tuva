{{ config(materialize='table') }}

select
    cast(encounter_id as varchar) as encounter_id
,   cast(patient_id as varchar) as patient_id
,   cast(encounter_type as varchar) as encounter_type
,   cast(encounter_start_date as date) as encounter_start_date
,   cast(encounter_end_date as date) as encounter_end_date
,   cast(admit_source as varchar) as admit_source
,   cast(admit_type as varchar) as admit_type
,   cast(discharge_disposition as varchar) as discharge_disposition
,   cast(physician_npi as varchar) as physician_npi
,   cast(location as varchar) as location
,   cast(location_npi as varchar) as location_npi
,   cast(ms_drg as varchar) as ms_drg
,   cast(paid_amount as float) as paid_amount
,   cast(data_source as varchar) as data_source
from {{ var('src_encounter') }}


