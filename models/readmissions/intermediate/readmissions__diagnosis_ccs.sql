{{ config(
     enabled = var('readmissions_enabled',var('tuva_marts_enabled',True))
   )
}}

-- Here we take all rows from the stg_diagnosis
-- table that correspond to encounters with
-- one and only one primary diagnosis code.
-- To each of these rows we also append:
--
--      - a 'valid_icd_10_cm_flag' to verify if it is a
--        valid ICD-10-CM code
--
--      - a 'ccs_diagnosis_category' column to indicate
--        the associated diagnosis category
--
-- encounter_ids that have no primary diagnosis or
-- multiple primary diagnoses are not part of this model
-- because it is impossible to assign a ccs_diagnosis_category
-- to them. In theory, each encounter should have a unique
-- ccs_diagnosis_category that is determined by their unique
-- primary diagnosis ICD-10-CM code.

select
    aa.encounter_id,
    aa.diagnosis_code,
    aa.diagnosis_rank,
    case
        when bb.icd_10_cm is null then 0
	else 1
    end as valid_icd_10_cm_flag,
    cc.ccs_diagnosis_category
from
    {{ ref('readmissions__diagnosis') }} aa
    left join {{ ref('terminology__icd_10_cm') }} bb
    on aa.diagnosis_code = bb.icd_10_cm
    left join {{ ref('readmissions__icd_10_cm_to_ccs') }} cc
    on aa.diagnosis_code = cc.icd_10_cm
    left join {{ ref('readmissions__primary_diagnosis_count') }} dd
    on aa.encounter_id = dd.encounter_id

where
    aa.diagnosis_rank = 1
    and
    dd.primary_dx_count = 1
