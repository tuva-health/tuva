{{ config(
     enabled = var('ccsr_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

select
     icd_10_pcs as code
    , icd_10_pcs_description as code_description
    , prccsr as ccsr_category
    , substring(prccsr, 1, 3) as ccsr_parent_category
    , prccsr_description as ccsr_category_description
    , clinical_domain
    , ont.section as procedure_section
    , ont.operation
    , ont.approach
    , ont.device
    , ont.qualifier
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('ccsr__prccsr_v2023_1_cleaned_map') }} as ccsr_map
left outer join {{ ref('terminology__icd10_pcs_cms_ontology') }} as ont
    on ccsr_map.icd_10_pcs = ont.icd10pcs_code
