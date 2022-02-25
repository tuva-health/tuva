
-- Every encounter should have one and only one primary
-- diagnosis. A potential data quality problem would
-- be to have encounters that have no primary diagnosis
-- or multiple primary diagnoses.
-- Here we list the count of primary diagnoses associated
-- with each encounter that has at least one primary diagnosis
-- in the stg_diagnosis table.


{{ config(materialized='view') }}



with primary_diagnosis_count as (
select
    encounter_id,
    count(*) as primary_dx_count
from {{ var('src_diagnosis') }}
where diagnosis_rank = 1
group by encounter_id
)



select *
from primary_diagnosis_count
