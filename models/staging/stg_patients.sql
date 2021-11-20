
{{ config(materialized='view') }}

select
    cast(patient_id as string) as patient_id
,   cast(gender_code as integer) as gender_code
,   to_date(birth_date) as birth_date
,   to_date(deceased_date) as deceased_date
from {{ source('source',var('patients_source')) }}