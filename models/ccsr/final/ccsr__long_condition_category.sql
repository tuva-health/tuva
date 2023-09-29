{{ config(
     enabled = var('ccsr_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with ccsr__dx_vertical_pivot as (
    
    select * from {{ ref('ccsr__dx_vertical_pivot') }} 

), condition as (
    
    select * from {{ ref('ccsr__stg_core__condition') }}

), dxccsr_body_systems as (

    select * from {{ ref('ccsr__dxccsr_v2023_1_body_systems') }}

)

select 
    condition.encounter_id,
    condition.claim_id,
    condition.patient_id,
    condition.normalized_code,
    ccsr__dx_vertical_pivot.code_description,
    condition.condition_rank,
    ccsr__dx_vertical_pivot.ccsr_parent_category,
    dxccsr_body_systems.body_system,
    dxccsr_body_systems.parent_category_description,
    ccsr__dx_vertical_pivot.ccsr_category,
    ccsr__dx_vertical_pivot.ccsr_category_description,
    ccsr__dx_vertical_pivot.ccsr_category_rank,
    ccsr__dx_vertical_pivot.is_ip_default_category,
    ccsr__dx_vertical_pivot.is_op_default_category,
    {{ var('dxccsr_version') }} as dxccsr_version,
    '{{ var('tuva_last_run')}}' as tuva_last_run
from condition
left join ccsr__dx_vertical_pivot
    on condition.normalized_code = ccsr__dx_vertical_pivot.code
left join dxccsr_body_systems using(ccsr_parent_category)

    