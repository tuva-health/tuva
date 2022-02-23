
-- Here we map every procedure code to its corresponding
-- CCS procedure category.
-- This model may list more than one CCS procedure category
-- per encounter_id. Different procedures associated with the
-- encounter may have different associated CCS procedure categories.


{{ config(materialized='view') }}




select
    aa.encounter_id,
    aa.procedure_code,
    case
        when bb.icd_10_pcs is null then 0
	else 1
    end as valid_icd_10_pcs,
    cc.ccs
from
    {{ var('src_procedure') }} aa
    left join {{ ref('icd_10_pcs') }} bb
    on aa.procedure_code = bb.icd_10_pcs
    left join {{ ref('ccs_icd_10_pcs') }} cc
    on aa.procedure_code = cc.icd_10_pcs
