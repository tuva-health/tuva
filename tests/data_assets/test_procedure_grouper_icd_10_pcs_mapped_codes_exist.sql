{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

-- Grouper coverage can lag the current ICD-10-PCS release. Unmapped terminology
-- codes intentionally remain valid and produce null grouper fields in Core.
-- Every code that is mapped must still resolve to the retained terminology.

with icd_10_pcs_codes as (

    select
        terminology__icd_10_pcs.icd_10_pcs as code
    from {{ ref('terminology__icd_10_pcs') }} as terminology__icd_10_pcs

)

select
    procedure_grouper_code_map.code
from {{ ref('tuva_procedure_grouper_code_map') }} as procedure_grouper_code_map
left join icd_10_pcs_codes
    on procedure_grouper_code_map.code = icd_10_pcs_codes.code
where procedure_grouper_code_map.code_system = 'icd-10-pcs'
  and icd_10_pcs_codes.code is null
