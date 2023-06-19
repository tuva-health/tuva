{{ config(
     enabled = var('readmissions_enabled',var('tuva_marts_enabled',True))
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
from {{ ref('readmissions__diagnosis_ccs') }}
where ccs_diagnosis_category in
    (select distinct ccs_diagnosis_category
     from {{ ref('readmissions__exclusion_ccs_diagnosis_category') }} )
)


select *, '{{ var('last_update')}}' as last_update
from exclusions
