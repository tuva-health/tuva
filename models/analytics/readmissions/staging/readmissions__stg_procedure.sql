
-- Staging model for the input layer:
-- stg_procedure input layer model.
-- This contains one row for every unique procedure each patient has.


{{ config(enabled=var('readmissions_enabled',var('tuva_packages_enabled',True))) }}



select
    cast(encounter_id as {{ dbt.type_string() }}) as encounter_id,
    cast(code as {{ dbt.type_string() }}) as procedure_code

from {{ var('procedure') }}
where code_type = 'icd-10-pcs'
