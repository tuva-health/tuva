
-- Staging model for the input layer:
-- stg_diagnosis input layer model.
-- This contains one row for every unique diagnosis each patient has.


{{ config(materialize='view') }}



select
    cast(encounter_id as varchar) as encounter_id,
    cast(diagnosis_code as varchar) as diagnosis_code,
    cast(diagnosis_rank as integer) as diagnosis_rank

from {{ var('src_diagnosis') }}


