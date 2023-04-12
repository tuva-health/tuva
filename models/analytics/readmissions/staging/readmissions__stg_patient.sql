
-- Staging model for the input layer:
-- stg_patient input layer model.
-- This contains one row for every unique patient in the dataset.


{{ config(enabled=var('readmissions_enabled',var('tuva_packages_enabled',True))) }}



select
    cast(patient_id as {{ dbt.type_string() }}) as patient_id,
    cast(gender as {{ dbt.type_string() }}) as gender,
    cast(birth_date as date) as birth_date

from {{ var('patient') }}


