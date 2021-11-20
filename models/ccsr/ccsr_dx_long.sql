{{ config(materialized='view') }}

with ccsr_union as (
select
    encounter_id
,   ccsr_1 as ccsr
from {{ ref('diagnosis_to_ccsr') }}
where ccsr_1 is not null

union 

select
    encounter_id
,   ccsr_2 as ccsr
from {{ ref('diagnosis_to_ccsr') }}
where ccsr_2 is not null

union

select
    encounter_id
,   ccsr_3 as ccsr
from {{ ref('diagnosis_to_ccsr') }}
where ccsr_3 is not null

union 

select
    encounter_id
,   ccsr_4 as ccsr
from {{ ref('diagnosis_to_ccsr') }}
where ccsr_4 is not null

union

select
    encounter_id
,   ccsr_5 as ccsr
from {{ ref('diagnosis_to_ccsr') }}
where ccsr_5 is not null

union 

select
    encounter_id
,   ccsr_6 as ccsr
from {{ ref('diagnosis_to_ccsr') }}
where ccsr_6 is not null
)

select 
    a.encounter_id
,   a.ccsr
,   d.ccsr_description
,   case
        when b.default_ccsr_inpatient is not null then 'Y'
        else 'N'
    end default_ccsr_inpatient_flag
,   case
        when c.default_ccsr_outpatient is not null then 'Y'
        else 'N'
    end default_ccsr_outpatient_flag
from ccsr_union a
left join {{ ref('diagnosis_to_ccsr') }} b
    on a.encounter_id = b.encounter_id
    and a.ccsr = b.default_ccsr_inpatient
left join {{ ref('diagnosis_to_ccsr') }} c
    on a.encounter_id = c.encounter_id
    and a.ccsr = c.default_ccsr_outpatient
left join {{ ref('ccsr_descriptions') }} d
    on a.ccsr = d.ccsr