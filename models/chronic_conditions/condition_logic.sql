
{{ config(materialized='view') }}

with patients as (
select
    a.patient_id
,   b.encounter_id
,   b.encounter_start_date
,   c.diagnosis_code
,   c.diagnosis_code_ranking
from {{ source('staging','patients') }}  a
left join {{ source('staging','encounters') }}  b
    on a.patient_id = b.patient_id    
left join {{ source('staging','diagnoses') }} c
    on b.encounter_id = c.encounter_id
)

select
    a.patient_id
,   a.encounter_id
,   a.encounter_start_date
,   condition_category
,   condition
from patients a
inner join {{ ref('chronic_conditions') }} b
    on a.diagnosis_code = b.code
    and a.diagnosis_code_ranking in (1,2)
    and b.condition = 'Acute Myocardial Infarction'
    and b.inclusion_type = 'Include'

    
union

select
    a.patient_id
,   a.encounter_id
,   a.encounter_start_date
,   condition_category
,   condition
from patients a
inner join {{ ref('chronic_conditions') }} b
    on a.diagnosis_code = b.code
    and a.diagnosis_code_ranking in (1,2)
    and b.condition = 'Atrial Fibrillation'
    and b.inclusion_type = 'Include'
    
union

select
    a.patient_id
,   a.encounter_id
,   a.encounter_start_date
,   condition_category
,   condition
from patients a
inner join {{ ref('chronic_conditions') }} b
    on a.diagnosis_code = b.code
    and a.diagnosis_code_ranking = 1
    and b.condition = 'Cataract'
    and b.inclusion_type = 'Include'

union

select
    a.patient_id
,   a.encounter_id
,   a.encounter_start_date
,   condition_category
,   condition
from patients a
inner join {{ ref('chronic_conditions') }} b
    on a.diagnosis_code = b.code
    and a.diagnosis_code_ranking = 1
    and b.condition = 'Glaucoma'
    and b.inclusion_type = 'Include'
