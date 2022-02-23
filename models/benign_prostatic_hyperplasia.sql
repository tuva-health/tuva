{{ config(materialized='view', tags='chronic conditions') }}

with patients as (
select
    a.patient_id
,   b.encounter_id
,   b.admit_date
,   c.diagnosis_code
,   c.diagnosis_rank
from {{ var('src_patient') }}  a
left join {{ var('src_encounter') }}  b
    on a.patient_id = b.patient_id    
left join {{ var('src_diagnosis') }} c
    on b.encounter_id = c.encounter_id
)

, inclusion_diagnoses as (
select
    a.patient_id
,   a.encounter_id
,   a.admit_date
,   condition_category
,   condition
from patients a
inner join {{ ref('chronic_conditions') }} b
    on a.diagnosis_code = b.code
    and b.condition = 'Benign Prostatic Hyperplasia'
    and b.inclusion_type = 'Include'
)

, exclusion_encounters as (
select distinct
   a.encounter_id
from patients a
inner join {{ ref('chronic_conditions') }} b
    on a.diagnosis_code = b.code
    and b.condition = 'Benign Prostatic Hyperplasia'
    and b.inclusion_type = 'Exclude'
)

select distinct
    a.patient_id
,   a.encounter_id
,   a.admit_date
,   a.condition_category
,   a.condition
from inclusion_diagnoses a
left join exclusion_encounters b
    on a.encounter_id = b.encounter_id
where b.encounter_id is null
