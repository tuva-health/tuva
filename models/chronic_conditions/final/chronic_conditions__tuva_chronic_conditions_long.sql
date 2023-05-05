{{ config(
     enabled = var('tuva_chronic_conditions_enabled',var('tuva_marts_enabled',True))
   )
}}

with condition_row_number as
(
    select 
        patient_id
        ,code
        ,condition_date
        ,row_number() over(partition by patient_id, code order by condition_date asc) as rn_asc
        ,row_number() over(partition by patient_id, code order by condition_date desc) as rn_desc
    from {{ ref('core__condition')}}
)
, patient_conditions as
(
    select 
        patient_id
        ,code as icd_10_cm
        ,max(case when rn_asc = 1 then condition_date end) as first_diagnosis_date
        ,max(case when rn_desc = 1 then condition_date end) as last_diagnosis_date    
    from condition_row_number
    group by 
        patient_id
        ,code
)  

select 
    pc.patient_id
    ,h.condition_family
    ,h.condition
    ,min(first_diagnosis_date) as first_diagnosis_date
    ,max(last_diagnosis_date) as last_diagnosis_date
from {{ref('chronic_conditions__tuva_chronic_conditions_hierarchy')}} h
inner join patient_conditions pc
    on h.icd_10_cm_code = pc.icd_10_cm
group by 
    pc.patient_id
    ,h.condition_family
    ,h.condition