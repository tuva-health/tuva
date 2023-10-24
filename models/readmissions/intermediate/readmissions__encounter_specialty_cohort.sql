{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

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

--ranking to eventually assign a cohort to encounters in multiple cohorts
with cohort_ranks as (
    select 'Surgery/Gynecology' as cohort, 1 as c_rank
    union all
    select 'Cardiorespiratory' as cohort, 2 as c_rank
    union all
    select 'Cardiovascular' as cohort, 3 as c_rank
    union all
    select 'Neurology' as cohort, 4 as c_rank
    union all
    select 'Medicine' as cohort, 5 as c_rank
)


--get all encounter ids in any procedure or diagnosis based cohorts
, all_encounter_cohorts as (

    --encounter ids in procedure based cohorts
    select proc.encounter_id, 1 as c_rank
    from {{ ref('readmissions__procedure_ccs') }} proc
    left join {{ ref('readmissions__surgery_gynecology_cohort') }} sgc
        on proc.procedure_code = sgc.icd_10_pcs
    left join {{ ref('readmissions__specialty_cohort') }} sgsc
        on proc.ccs_procedure_category = sgsc.ccs and sgsc.specialty_cohort = 'Surgery/Gynecology'
    where sgc.icd_10_pcs is not null or sgsc.ccs is not null

    union all

    --encounter ids in diagnosis based cohorts
    select diag.encounter_id, cohort_ranks.c_rank
    from {{ ref('readmissions__encounter_with_ccs') }} diag
    inner join {{ ref('readmissions__specialty_cohort') }} sc
        on diag.ccs_diagnosis_category = sc.ccs and sc.procedure_or_diagnosis = 'Diagnosis'
    inner join cohort_ranks
        on sc.specialty_cohort = cohort_ranks.cohort
)


-- getting one cohort per encounter
, main_encounter_cohort as (
    select encounter_id, min(c_rank) as main_c_rank
    from all_encounter_cohorts
    group by encounter_id

)


--getting all encounters, with labeled cohorts, if no cohort cohort is "medicine"
select enc.encounter_id, coalesce(cohort_ranks.cohort, 'Medicine') as specialty_cohort, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('readmissions__encounter') }} enc
left join main_encounter_cohort mec
    on enc.encounter_id = mec.encounter_id
left join cohort_ranks
    on mec.main_c_rank = cohort_ranks.c_rank

