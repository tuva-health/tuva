{{ config(materialized='table') }}

select
    cast(encounter_id as varchar) as encounter_id
,   cast(patient_id as varchar) as patient_id
,   cast(encounter_type as varchar) as encounter_type
,   cast(encounter_start_date as date) as encounter_start_date
,   cast(encounter_end_date as date) as encounter_end_date
,   cast(admit_source_code as varchar) as admit_source_code
,   cast(admit_source_description as varchar) as admit_source_description
,   cast(admit_type_code as varchar) as admit_type_code
,   cast(admit_type_description as varchar) as admit_type_description
,   cast(discharge_disposition_code as varchar) as discharge_disposition_code
,   cast(discharge_disposition_description as varchar) as discharge_disposition_description
,   cast(physician_npi as varchar) as physician_npi
,   cast(location as varchar) as location
,   cast(facility_npi as varchar) as facility_npi
,   cast(ms_drg as varchar) as ms_drg
,   cast(paid_amount as float) as paid_amount
,   cast(data_source as varchar) as data_source
from {{ var('src_encounter') }}
