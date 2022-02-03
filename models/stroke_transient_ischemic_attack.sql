{{ config(materialized='table', tags='chronic conditions') }}

with patients as (
select
    a.patient_id
,   b.encounter_id
,   b.encounter_start_date
,   c.diagnosis_code
,   c.diagnosis_rank
from {{ ref('stg_patient') }}  a
left join {{ ref('stg_encounter') }}  b
    on a.patient_id = b.patient_id    
left join {{ ref('stg_diagnosis') }} c
    on b.encounter_id = c.encounter_id
)

, inclusion_diagnoses as (
select
    a.patient_id
,   a.encounter_id
,   a.encounter_start_date
,   condition_category
,   condition
from patients a
inner join {{ ref('chronic_conditions') }} b
    on a.diagnosis_code = b.code
    and b.condition = 'Stroke/Transient Ischemic Attack'
    and b.inclusion_type = 'Include'
)

, exclusion_encounters as (
select distinct
   a.encounter_id
from patients a
inner join {{ ref('chronic_conditions') }} b
    on a.diagnosis_code = b.code
    and b.condition = 'Stroke/Transient Ischemic Attack'
    and b.inclusion_type = 'Exclude'
)

select distinct
    a.patient_id
,   a.encounter_id
,   a.encounter_start_date
,   a.condition_category
,   a.condition
from inclusion_diagnoses a
left join exclusion_encounters b
    on a.encounter_id = b.encounter_id
where b.encounter_id is null
