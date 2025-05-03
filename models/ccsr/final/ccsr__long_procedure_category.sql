{{ config(
     enabled = var('ccsr_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with procedures as (

    select * from {{ ref('ccsr__stg_core__procedure') }}

)

, ccsr__procedure_category_map as (

    select * from {{ ref ('ccsr__procedure_category_map') }}

)

select distinct
      procedures.encounter_id
    , procedures.claim_id
    , procedures.person_id
    , procedures.normalized_code
    , ccsr__procedure_category_map.code_description
    , ccsr__procedure_category_map.ccsr_parent_category
    , ccsr__procedure_category_map.ccsr_category
    , ccsr__procedure_category_map.ccsr_category_description
    , ccsr__procedure_category_map.clinical_domain
    , ccsr__procedure_category_map.procedure_section
    , ccsr__procedure_category_map.operation
    , ccsr__procedure_category_map.approach
    , ccsr__procedure_category_map.device
    , ccsr__procedure_category_map.qualifier
    , {{ var('prccsr_version') }} as prccsr_version
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from procedures
left outer join ccsr__procedure_category_map
    on procedures.normalized_code = ccsr__procedure_category_map.code
