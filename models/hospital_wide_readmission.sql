{{ config(materialized='view') }}

with index_admissions as (
select
    encounter_id
from {{ ref('encounters') }}
where discharge_status_code not in ('02','07','20')
)

, encounter_sequence as (
select
    row_number() over(partition by patient_id order by discharge_date) as encounter_sequence
,   *
from {{ ref('encounters') }}
)

, readmit_calc as (
select
    a.patient_id,
    a.encounter_id,
    (b.AdmissionDate - a.DischargeDate) AS days_to_readmit,
    1 as readmit_flag
from encounter_sequence a
inner join encounter_sequence b
    on a.patient_id = b.patient_id
    and a.encounter_sequence + 1 = b.encounter_sequence
)

select
    case
        when b.readmit_flag = 1 then 1 
        else 0
    end as readmit_flag,
    b.days_to_readmit,
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
        when b.days_to_readmit < 366 then 1 
        else 0
    end as readmit_365_flag,
    a.*
from encounter_sequence a
left join readmit_calc b
    on a.encounter_id = b.encounter_id
inner join index_admissions c
    on a.encounter_id = c.encounter_id
