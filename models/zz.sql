{{ config(materialized='view', tags='readmissions') }}

with index_admissions as (
select
    encounter_id,
    1 as index_admit_flag,
from {{ ref('inpatient_encounter') }}
),

admission_sequence as (
select
    encounter_id,
    patient_id,
    admit_date,
    discharge_date,
    row_number() over(partition by patient_id order by discharge_date) as admission_sequence
from {{ ref('inpatient_encounter') }}
),

readmit_calc as (
select
    a.patient_id,
    a.encounter_id,
    a.encounter_id as readmit_encounter_id,
    (b.admit_date - a.discharge_date) AS days_to_readmit,
    1 as readmit_flag
from admission_sequence a
inner join admission_sequence b
    on a.patient_id = b.patient_id
    and a.admission_sequence + 1 = b.admission_sequence
),

hospital_wide_readmission as (
select
    a.encounter_id,
    a.admission_sequence,
    a.admit_date,
    a.discharge_date,
    a.patient_id,
    c.index_admit_flag,
    b.days_to_readmit,
    b.readmit_encounter_id,
    case
        when b.readmit_flag = 1 then 1 
        else 0
    end as readmit_flag,
    case
        when b.days_to_readmit = 0 then 1 
        else 0
    end as readmit_0_flag,
    case
        when b.days_to_readmit = 1 then 1 
        else 0
    end as readmit_1_flag,
    case
        when b.days_to_readmit < 8 then 1 
        else 0
    end as readmit_7_flag,
    case
        when b.days_to_readmit < 16 then 1 
        else 0
    end as readmit_15_flag,
    case
        when b.days_to_readmit < 31 then 1 
        else 0
    end as readmit_30_flag,
    case
        when b.days_to_readmit < 91 then 1 
        else 0
    end as readmit_90_flag,
    case
        when b.days_to_readmit < 181 then 1 
        else 0
    end as readmit_180_flag,
    case
        when b.days_to_readmit < 366 then 1 
        else 0
    end as readmit_365_flag
from admission_sequence a
left join readmit_calc b
    on a.encounter_id = b.encounter_id
inner join index_admissions c
    on a.encounter_id = c.encounter_id
)

select *
from hospital_wide_readmission
