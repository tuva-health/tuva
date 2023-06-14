{{ config(
     enabled = var('ccsr_enabled',var('tuva_marts_enabled',True))
   )
}}

select 
    icd_10_pcs as code,
    icd_10_pcs_description as code_description,
    prccsr as ccsr_category,
    left(prccsr, 3) as ccsr_parent_category,
    prccsr_description as ccsr_category_description,
    clinical_domain,
   '{{ var('last_update')}}' as last_update
from {{ ref('ccsr__prccsr_v2023_1_cleaned_map')}}