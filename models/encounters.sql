
{{ config(materialized='view') }}

select
    encounter_id
,   patient_id
,   encounter_start_date
,   encounter_end_date
,   encounter_type
,   admit_type_code
,   admit_source_code
,   discharge_status_code
,   attending_provider_npi
,   facility_npi
,   drg
,   paid_amount
from {{ var('src_encounters') }}

