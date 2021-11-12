
{{ config(materialized='view') }}

with patients as (
select
    a.patient_id
,   b.encounter_id
,   b.encounter_start_date
from {{ ref('patients') }} a
left join {{ ref('encounters') }}  b
    on a.patient_id = b.patient_id    
)

, diagnosis_conditions as (
select
    a.patient_id
,   a.encounter_id
,   a.encounter_start_date
,   c.condition_category
,   c.condition
from patients a
inner join {{ ref('diagnoses') }}  b
    on a.encounter_id = b.encounter_id
inner join {{ ref('chronic_conditions') }}  c
    on b.diagnosis_code = c.code
    and c.code_type = 'ICD-10-CM'
    and c.inclusion_type = 'Include'
    and c.additional_logic = 'None'
)
    
, procedure_conditions as (
select
    a.patient_id
,   a.encounter_id
,   a.encounter_start_date
,   c.condition_category
,   c.condition
from patients a
inner join {{ ref('procedures') }}  b
    on a.encounter_id = b.encounter_id
inner join {{ ref('chronic_conditions') }}  c
    on b.procedure_code = c.code
    and c.code_type = 'ICD-10-PCS'
    and c.inclusion_type = 'Include'
    and c.additional_logic = 'None'
)

select *
from diagnosis_conditions

union

select *
from procedure_conditions