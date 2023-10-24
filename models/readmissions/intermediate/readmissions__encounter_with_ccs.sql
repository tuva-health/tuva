{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

-- Here we add a CCS diagnosis category to
-- every encounter that we can add a CCS diagnosis category to.
-- The CCS diagnosis category is found using
-- the encounter's primary diagnosis code.


select
    aa.encounter_id,
    aa.patient_id,
    aa.admit_date,
    aa.discharge_date,
    aa.discharge_disposition_code,
    aa.facility_npi,
    aa.ms_drg_code,
    aa.paid_amount,
    aa.primary_diagnosis_code,

    case
      when bb.icd_10_cm is not null then 1
      else 0
    end as valid_primary_diagnosis_code_flag,

    cc.ccs_diagnosis_category,

    '{{ var('tuva_last_run')}}' as tuva_last_run

from
    {{ ref('readmissions__encounter') }} aa
    left join {{ ref('terminology__icd_10_cm') }} bb
    on aa.primary_diagnosis_code = bb.icd_10_cm
    left join {{ ref('readmissions__icd_10_cm_to_ccs') }} cc
    on aa.primary_diagnosis_code = cc.icd_10_cm
