
-- Here we list all index admissions for the hospital wide readmissions
-- measure.
-- These represent encounter_ids that meet the requirements to be an
-- index admission for the HWR measure.
-- These are the requirements for a hospitalization to be an index admission
-- for the HWR measure:
--
--     Time Requirement: The discharge data must be at least 30 days
--                       earlier than the last dischareg date available
--                       in the dataset.
-- 
--     Discharge Requirements: The patient must not be discharged to another
--                             acute care hospital; the patient must not have
--                             left against medical advice; and the patient
--                             must be alive at discharge.
--
--     Diagnosis Requirements: Exclude encounters where based on the CCS
--     (exclusions)            diagnosis category we know the encounter was
--                             for medical treatment of cancer, rehabilitation,
--                             or psychiatric reasons.


{{ config(materialized='view') }}




select distinct encounter_id
from {{ var('src_encounter') }}
where 
    encounter_id in (select *
	             from {{ ref('index_time_requirements') }} )
    and
    encounter_id in (select *
	             from {{ ref('index_discharge_requirements') }} )
    and
    encounter_id not in (select *
	                 from {{ ref('exclusions') }} )
