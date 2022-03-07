
-- Staging model for the input layer:
-- stg_procedure input layer model.
-- This contains one row for every unique procedure each patient has.


{{ config(materialize='view') }}



select
    cast(encounter_id as varchar) as encounter_id,
    cast(procedure_code as varchar) as procedure_code

from {{ var('src_procedure') }}
