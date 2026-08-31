{{ config(
     tags = ['data_assets', 'package_invariant', 'condition_grouper'],
     severity = 'error'
   )
}}

-- Grouper coverage can lag the current ICD-10-CM release. Unmapped terminology
-- codes intentionally remain valid and produce null grouper fields in Core.
-- Every code that is mapped must still resolve to the retained terminology.

with icd_10_cm_codes as (

    select
        terminology__icd_10_cm.icd_10_cm as code
    from {{ ref('terminology__icd_10_cm') }} as terminology__icd_10_cm

)

select
    condition_grouper_code_map.code
from {{ ref('tuva_condition_grouper_code_map') }} as condition_grouper_code_map
left join icd_10_cm_codes
    on condition_grouper_code_map.code = icd_10_cm_codes.code
where condition_grouper_code_map.code_system = 'icd-10-cm'
  and icd_10_cm_codes.code is null
