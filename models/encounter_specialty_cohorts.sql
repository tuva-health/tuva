
-- Here we list the specialty cohort for each encounter that has
-- an associated specialty cohort.
-- There are 5 possible specialty cohorts:
--      [1] Medicine
--      [2] Surgery/Gynecology
--      [3] Cardiology
--      [4] Cardiovascular
--      [5] Neurology
-- An encounter that has an ICD-10-PCS procedure code or a
-- CCS procedure category that corresponds to the
-- 'Surgery/Gynecology' cohort will always be in that cohort.
-- For encounters that are not in the 'Surgery/Gynecology' cohort,
-- we then check to see if they are in one of the other 4 cohorts.


{{ config(materialized='view') }}



-- All encounter_ids that have an ICD-10-PCS procedure code
-- or a CCS procedure category that corresponds to the
-- 'Surgery/Gynecology' cohort
with surgery_gynecology as (
select distinct encounter_id
from {{ ref('procedure_ccs') }}
where
    procedure_code in (select distinct icd_10_pcs
                       from {{ ref('surgery_gynecology_cohort') }} )
    or
    ccs in (select distinct ccs
            from {{ ref('specialty_cohorts') }}
	    where specialty_cohort = 'Surgery/Gynecology' )
),


-- All encounter_ids that are not in the 'Surgery/Gynecology' cohort
-- and are in the 'Medicine' cohort
medicine as (
select distinct encounter_id
from {{ ref('diagnosis_ccs') }}
where
    diagnosis_rank = 1
    and
    encounter_id not in (select * from surgery_gynecology)
    and
    ccs in (select distinct ccs
            from {{ ref('specialty_cohorts') }}
	    where specialty_cohort = 'Medicine' )
),


-- All encounter_ids that are not in the 'Surgery/Gynecology' cohort
-- and are in the 'Cardiorespiratory' cohort
cardiorespiratory as (
select distinct encounter_id
from {{ ref('diagnosis_ccs') }}
where
    diagnosis_rank = 1
    and
    encounter_id not in (select * from surgery_gynecology)
    and
    ccs in (select distinct ccs
            from {{ ref('specialty_cohorts') }}
	    where specialty_cohort = 'Cardiorespiratory' )    
),


-- All encounter_ids that are not in the 'Surgery/Gynecology' cohort
-- and are in the 'Cardiovascular' cohort
cardiovascular as (
select distinct encounter_id
from {{ ref('diagnosis_ccs') }}
where
    diagnosis_rank = 1
    and
    encounter_id not in (select * from surgery_gynecology)
    and
    ccs in (select distinct ccs
            from {{ ref('specialty_cohorts') }}
	    where specialty_cohort = 'Cardiovascular' )    
),


-- All encounter_ids that are not in the 'Surgery/Gynecology' cohort
-- and are in the 'Neurology' cohort
neurology as (
select distinct encounter_id
from {{ ref('diagnosis_ccs') }}
where
    diagnosis_rank = 1
    and
    encounter_id not in (select * from surgery_gynecology)
    and
    ccs in (select distinct ccs
            from {{ ref('specialty_cohorts') }}
	    where specialty_cohort = 'Neurology' )    
),


-- All encounter_ids that have an associated cohort listed
-- with their corresponding cohort
all_cohorts as (
select encounter_id, 'Surgery/Gynecology' as specialty_cohort
from surgery_gynecology
union
select encounter_id, 'Medicine' as specialty_cohort
from medicine
union
select encounter_id, 'Cardiorespiratory' as specialty_cohort
from cardiorespiratory
union
select encounter_id, 'Cardiovascular' as specialty_cohort
from cardiovascular
union
select encounter_id, 'Neurology' as specialty_cohort
from neurology
),


-- Assign a specialty cohort to ALL encounters. If an encounter
-- does not belong to any specialty cohort according to the
-- rules above, then it is assigned to the 'Medicine' cohort
-- by default
cohorts_for_all_encounters as (
select
    aa.encounter_id,
    case
        when bb.specialty_cohort is not null then bb.specialty_cohort
	else 'Medicine'
    end as specialty_cohort
from {{ var('src_encounter') }} aa
     left join all_cohorts bb on aa.encounter_id = bb.encounter_id
)



select *
from cohorts_for_all_encounters
