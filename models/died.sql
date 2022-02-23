
-- Here we list encounter_ids where the patient died
-- (discharge_status_code = 20)


{{ config(materialized='view') }}




-- Encounters where patient died
with died as (
select encounter_id
from {{ var('src_encounter') }}
where discharge_status_code = '20'   
)


select *
from died
