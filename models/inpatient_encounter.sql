{{ config(materialized='view', tags='readmissions') }}

select
    encounter_id,
    patient_id,
    encounter_start_date as admit_date,
    encounter_end_date as discharge_date,
    discharge_status_code,
    case
        when admit_type_code = 3 then 1
        else 0
    end as elective_flag,
    case
        when discharge_status_code between 81 and 95 then 1
        else 0
    end as planned_readmission_flag,
    case
        when discharge_status_code = 01 then 'home'
        when discharge_status_code = 06 then 'home health'
        when discharge_status_code = 62 then 'inpatient rehab facility'
        when discharge_status_code in (03,64) then 'skilled nursing facility'
        when discharge_status_code = 65 then 'inpatient psychiatric facility'
        when discharge_status_code in (50,51) then 'hospice'
        when discharge_status_code = 02 then 'other acute care hospital'
        when discharge_status_code = 07 then 'left against medical advice'
        else 'other'
    end as discharged_to,
    case
        when discharge_status_code = 20 then 1
        else 0
    end as inpatient_mortality_flag,
    case
        when discharge_status_code in (20,40,41) then 1
        else 0
    end as mortality_flag,
    cast(a.drg as string) || ': ' || b.description as drg,
    cast(b.mdc as string) || ': ' || c.description as mdc,
    b.medical_surgical as drg_type
from {{ ref('stg_encounter') }} a
left join {{ ref('ms_drg') }} b
    on a.drg = b.drg
left join {{ ref('mdc') }} c
    on b.mdc = c.code
where a.encounter_type = 'acute inpatient'