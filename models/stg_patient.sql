
-- Staging model for the input layer:
-- stg_patient input layer model.
-- This contains one row for every unique patient in the dataset.


{{ config(materialize='view') }}



select
    cast(patient_id as varchar) as patient_id,
    cast(gender as varchar) as gender,
    cast(birth_date as date) as birth_date

from {{ var('src_patient') }}


