
-- Here we map every procedure code to its corresponding
-- CCS procedure category.
-- This model may list more than one CCS procedure category
-- per encounter_id because different procedures associated with the
-- encounter (different rows on the stg_procedure model) may have
-- different associated CCS procedure categories.


{{ config(enabled=var('readmissions_enabled',var('tuva_packages_enabled',True))) }}




select
    aa.encounter_id,
    aa.procedure_code,
    case
        when bb.icd_10_pcs is null then 0
	else 1
    end as valid_icd_10_pcs_flag,
    cc.ccs_procedure_category
from
    {{ ref('readmissions__stg_procedure') }} aa
    left join {{ ref('terminology__icd_10_pcs') }} bb
    on aa.procedure_code = bb.icd_10_pcs
    left join {{ ref('readmissions__icd_10_pcs_to_ccs') }} cc
    on aa.procedure_code = cc.icd_10_pcs
