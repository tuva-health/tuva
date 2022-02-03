{{ config(materialized='table', tags='ccsr') }}

select 
    a.encounter_id
,   case
        when a.encounter_type = 'Acute Inpatient' then c.default_ccsr_inpatient
        else null
    end default_ccsr_inpatient
,   case
        when a.encounter_type = 'Outpatient' then c.default_ccsr_outpatient
        else null
    end default_ccsr_outpatient
,   c.ccsr_1
,   c.ccsr_2
,   c.ccsr_3
,   c.ccsr_4
,   c.ccsr_5
,   c.ccsr_6
from {{ ref('stg_encounter') }} a
left join {{ ref('stg_diagnosis') }} b
    on a.encounter_id = b.encounter_id
left join {{ ref('ccsr_dx_mapping') }} c
    on b.diagnosis_code = c.diagnosis_code