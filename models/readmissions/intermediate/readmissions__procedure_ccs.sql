{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

-- Here we map every procedure code to its corresponding
-- CCS procedure category.
-- This model may list more than one CCS procedure category
-- per encounter_id because different procedures associated with the
-- encounter (different rows on the stg_procedure model) may have
-- different associated CCS procedure categories.



select
    aa.encounter_id,
    aa.normalized_code as procedure_code,
    case
        when bb.icd_10_pcs is null then 0
	else 1
    end as valid_icd_10_pcs_flag,
    cc.ccs_procedure_category,
    '{{ var('tuva_last_run')}}' as tuva_last_run
from
    {{ ref('readmissions__stg_core__procedure') }} aa
    left join {{ ref('terminology__icd_10_pcs') }} bb
    on aa.normalized_code = bb.icd_10_pcs
    left join {{ ref('readmissions__icd_10_pcs_to_ccs') }} cc
    on aa.normalized_code = cc.icd_10_pcs
where aa.normalized_code_type = 'icd-10-pcs'
