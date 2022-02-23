
-- Here we list encounter_ids for all encounters
-- that are planned.


{{ config(materialized='view') }}


-- encounter_ids for encounters that we know
-- are planned because they had a procedure category
-- that is only present for planned encounters 
with always_planned_px as (
select distinct encounter_id
from {{ ref('procedure_ccs') }}
where ccs in (select distinct ccs_procedure_category
              from {{ ref('always_planned_px') }} )
),


-- encounter_ids for encounters that we know
-- are planned because they had a diagnosis category
-- that is only present for planned encounters
always_planned_dx as (
select distinct encounter_id
from {{ ref('diagnosis_ccs') }}
where ccs in (select distinct ccs_diagnosis_category
              from {{ ref('always_planned_dx') }} )
),


-- encounter_ids for encounters that are potentially planned
-- based on one of their CCS procedure categories.
-- For these encounters to actually be planned, we must further
-- require that they are not acute encounters
potentially_planned_px_ccs as (
select distinct encounter_id
from {{ ref('procedure_ccs') }}
where ccs in (select distinct ccs_procedure_category
              from {{ ref('potentially_planned_px_ccs') }} )
),


-- encounter_ids for encounters that are potentially planned
-- based on their ICD-10-PCS procedure codes.
-- For these encounters to actually be planned, we must further
-- require that they are not acute encounters
potentially_planned_px_icd10pcs as (
select distinct encounter_id
from {{ ref('procedure_ccs') }}
where procedure_code in (select distinct icd10pcs
                         from {{ ref('potentially_planned_px_icd10pcs') }} )
),


-- encounter_ids for encounters that are acute based
-- on their primary diagnosis code or their CCS diagnosis category
acute_encounters as (
select distinct encounter_id
from {{ ref('diagnosis_ccs') }}
where diagnosis_code in (select distinct icd10cm
                         from {{ ref('acute_diagnoses_icd10cm') }})
      or
      ccs in (select distinct ccs_diagnosis_category
              from {{ ref('acute_diagnoses_ccs') }})
),


-- encounter_ids for encounters that are:
--           [1] potentially planned, based on one of
--               their CCS procedure categories or
--           [2] acute, based on their primary diagnosis code
--               or their CCS diagnosis category
-- These encounters are therefore confirmed to be planned
potentially_planned_that_are_actually_planned as (
select *
from potentially_planned_px_ccs
where encounter_id not in (select * from acute_encounters)
union
select *
from potentially_planned_px_icd10pcs
where encounter_id not in (select * from acute_encounters)
),


-- Aggregate of all encounter_ids for planned encounters
all_planned_encounters as (
select * from always_planned_px
union
select * from always_planned_dx
union
select * from potentially_planned_that_are_actually_planned
)




select *
from all_planned_encounters
