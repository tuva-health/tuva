{{ config(
     tags = ['data_assets', 'package_invariant', 'condition_grouper'],
     severity = 'error'
   )
}}

-- Mapping validity is independent of terminology lifecycle. Current and
-- deprecated SNOMED CT concepts may both be mapped, but every mapped concept
-- must remain present in the retained terminology relation.

with snomed_ct_codes as (

    select
        terminology__snomed_ct.snomed_ct as code
    from {{ ref('terminology__snomed_ct') }} as terminology__snomed_ct

)

select
    condition_grouper_code_map.code
from {{ ref('tuva_condition_grouper_code_map') }} as condition_grouper_code_map
left join snomed_ct_codes
    on condition_grouper_code_map.code = snomed_ct_codes.code
where condition_grouper_code_map.code_system = 'snomed-ct'
  and snomed_ct_codes.code is null
