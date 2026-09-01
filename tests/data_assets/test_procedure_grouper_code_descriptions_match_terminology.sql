{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

select
    code_map.code_system
  , code_map.code
  , code_map.code_description
  , terminology.description as terminology_description
from {{ ref('tuva_procedure_grouper_code_map') }} as code_map
inner join {{ ref('terminology__icd_10_pcs') }} as terminology
  on code_map.code = terminology.icd_10_pcs
where code_map.code_system = 'icd-10-pcs'
  and (
       code_map.code_description is null
       or terminology.description is null
       or code_map.code_description != terminology.description
  )
