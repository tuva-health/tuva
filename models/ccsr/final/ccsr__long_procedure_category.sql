{{ config(
     enabled = var('ccsr_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with procedure as (
    
    select * from {{ ref('ccsr__stg_core__procedure') }}

), ccsr__procedure_category_map as (

    select * from {{ ref ('ccsr__procedure_category_map') }}

)

select distinct
    procedure.encounter_id,
    procedure.patient_id,
    procedure.normalized_code,
    ccsr__procedure_category_map.code_description,
    ccsr__procedure_category_map.ccsr_parent_category,
    ccsr__procedure_category_map.ccsr_category,
    ccsr__procedure_category_map.ccsr_category_description,
    ccsr__procedure_category_map.clinical_domain,
    {{ var('prccsr_version') }} as prccsr_version,
    '{{ var('tuva_last_run')}}' as tuva_last_run
from procedure
left join ccsr__procedure_category_map
    on procedure.normalized_code = ccsr__procedure_category_map.code