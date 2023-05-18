{{ config(
     enabled = var('ccsr_enabled',var('tuva_marts_enabled',True))
   )
}}

with procedure as (
    
    select * from {{ ref('core__procedure') }}

), ccsr__procedure_category_map as (

    select * from {{ ref ('ccsr__procedure_category_map') }}

)

select distinct
    procedure.encounter_id,
    procedure.patient_id,
    procedure.code,
    ccsr__procedure_category_map.code_description,
    ccsr__procedure_category_map.ccsr_parent_category,
    ccsr__procedure_category_map.ccsr_category,
    ccsr__procedure_category_map.ccsr_category_description,
    ccsr__procedure_category_map.clinical_domain,
    {{ var('prccsr_version') }} as prccsr_version,
    '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as _model_run_time
from procedure
left join ccsr__procedure_category_map using(code)