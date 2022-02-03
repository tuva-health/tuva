{{ config(materialized='table', tags='chronic conditions') }}

with condition_union as (
select *
from {{ ref('condition_logic_simple') }}

union

select *
from {{ ref('condition_logic') }}

union

select *
from {{ ref('benign_prostatic_hyperplasia') }}

union

select *
from {{ ref('stroke_transient_ischemic_attack') }}
)

select
    patient_id
,   condition_category
,   condition
,   min(encounter_start_date) as condition_onset_date
,   max(encounter_start_date) as condition_recent_date
,   count(encounter_id) as condition_count
from condition_union
group by 1,2,3