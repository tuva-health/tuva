
-- Here we take all diagnosis codes from the stg_diagnosis model
-- and append:
--
--      - a 'valid_icd_10_cm' flag to verify if it is a
--        valid ICD-10-CM code
--
--      - a 'ccs' column to indicate the associated
--        diagnosis category


{{ config(materialized='view') }}



select
    aa.encounter_id,
    aa.diagnosis_code,
    aa.diagnosis_rank,
    case
        when bb.icd_10_cm is null then 0
	else 1
    end as valid_icd_10_cm,
    cc.ccs
from
    {{ var('src_diagnosis') }} aa
    left join {{ ref('icd_10_cm') }} bb
    on aa.diagnosis_code = bb.icd_10_cm
    left join {{ ref('ccs_icd_10_cm') }} cc
    on aa.diagnosis_code = cc.icd_10_cm

