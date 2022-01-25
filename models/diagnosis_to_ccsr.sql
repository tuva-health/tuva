{{ config(materialized='view', tags='ccsr') }}

select 
    a.encounter_id
,   case
        when a.encounter_type = 'inpatient' then c.default_ccsr_inpatient
        else null
    end default_ccsr_inpatient
,   case
        when a.encounter_type = 'outpatient' then c.default_ccsr_outpatient
        else null
    end default_ccsr_outpatient
,   c.ccsr_1
,   c.ccsr_2
,   c.ccsr_3
,   c.ccsr_4
,   c.ccsr_5
,   c.ccsr_6
from {{ ref('encounters') }} a
left join {{ ref('diagnoses') }} b
    on a.encounter_id = b.encounter_id
left join {{ ref('ccsr_dx_mapping') }} c
    on b.diagnosis_code = c.diagnosis_code