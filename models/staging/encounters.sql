
{{ config(materialized='view') }}

select
    cast(encounter_id as string) as encounter_id,
    cast(member_id as string) as patient_id,
    to_date(encounter_start_date) as encounter_start_date,
    to_date(encounter_end_date) as encounter_end_date,
    cast(admit_type_code as integer) as admit_type_code,
    cast(admit_source_code as integer) as admit_source_code,
    cast(discharge_status_code as integer) as discharge_status_code
from hcup.public.encounters
