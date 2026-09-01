{{ config(
     tags = ['data_assets', 'package_invariant', 'procedure_grouper'],
     severity = 'error'
   )
}}

-- Procedure Grouper scope is intentionally ICD-10-PCS-only. Coverage can lag
-- the current terminology release; the exact reviewed FY2027 gap is enforced
-- separately. Every retained mapping, including mappings for deprecated codes,
-- must still resolve to the retained terminology.

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
