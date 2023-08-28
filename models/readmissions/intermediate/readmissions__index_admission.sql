{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

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



select distinct a.encounter_id, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('readmissions__encounter') }} a
inner join {{ ref('readmissions__index_time_requirement') }} b
    on a.encounter_id = b.encounter_id
inner join {{ ref('readmissions__index_discharge_requirement') }} c
    on a.encounter_id = c.encounter_id
left join {{ ref('readmissions__exclusion') }} d
    on a.encounter_id = d.encounter_id
where d.encounter_id is null

