{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

-- Terminology deprecation does not retire a reviewed mapping. Historical
-- ICD-10-PCS codes remain grouped for longitudinal analytics.

select
    terminology.icd_10_pcs as code
  , terminology.description
from {{ ref('terminology__icd_10_pcs') }} as terminology
left join {{ ref('tuva_procedure_grouper_code_map') }} as code_map
  on terminology.icd_10_pcs = code_map.code
  and code_map.code_system = 'icd-10-pcs'
  and code_map.status = 'active'
where terminology.deprecated = 1
  and code_map.code is null
