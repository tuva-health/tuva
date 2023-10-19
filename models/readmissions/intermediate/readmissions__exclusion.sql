{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

-- Here we list encounter_ids that are excluded
-- from being index admissions because they
-- belong to one of these categories:
--       [1] Medical Treatment of Cancer
--       [2] Rehabilitation
--       [3] Psychiatric


-- encounter_ids for encounters that should be
-- excluded because they belong to one of the
-- exclusion categories
with exclusions as (
select distinct encounter_id
from {{ ref('readmissions__encounter_with_ccs') }}
where
(ccs_diagnosis_category is not null)
and
(
ccs_diagnosis_category in
    (select distinct ccs_diagnosis_category
     from {{ ref('readmissions__exclusion_ccs_diagnosis_category') }} )
)
)


select *, '{{ var('tuva_last_run')}}' as tuva_last_run
from exclusions
