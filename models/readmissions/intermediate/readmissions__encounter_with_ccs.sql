{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

-- Here we add a CCS diagnosis category to
-- every encounter that we can add a CCS diagnosis category to.
-- The CCS diagnosis category is found using
-- the encounter's primary diagnosis code.


select
    aa.encounter_id
    , aa.person_id
    , aa.admit_date
    , aa.discharge_date
    , aa.discharge_disposition_code
    , aa.facility_id
    , aa.drg_code_type
    , aa.drg_code
    , aa.paid_amount
    , aa.primary_diagnosis_code

    , case
      when bb.icd_10_cm is not null then 1
      else 0
    end as valid_primary_diagnosis_code_flag

    , cc.ccs_diagnosis_category

    , '{{ var('tuva_last_run') }}' as tuva_last_run

from
    {{ ref('readmissions__encounter') }} as aa
    left outer join {{ ref('terminology__icd_10_cm') }} as bb
    on aa.primary_diagnosis_code = bb.icd_10_cm
    left outer join {{ ref('readmissions__icd_10_cm_to_ccs') }} as cc
    on aa.primary_diagnosis_code = cc.icd_10_cm
