-- Here we add a CCS diagnosis category to
-- every encounter that we can add a CCS diagnosis category to.
-- The CCS diagnosis category is found using
-- the encounter's primary diagnosis code.

with core__encounter as (
    select *
    from {{ ref('core__encounter') }}
)
, icd_10_cm as (
    select *
    from {{ ref('tuva_data_assets', 'icd_10_cm') }}
)
, icd_10_cm_to_ccs as (
    select *
    from {{ ref('tuva_data_assets', 'icd_10_cm_to_ccs') }}
)
select
    aa.encounter_sk
    , aa.member_id
    , aa.encounter_start_date as admit_date
    , aa.encounter_end_date as discharge_date
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
from core__encounter as aa
    left outer join icd_10_cm as bb
    on aa.primary_diagnosis_code = bb.icd_10_cm
    left outer join icd_10_cm_to_ccs as cc
    on aa.primary_diagnosis_code = cc.icd_10_cm
where aa.encounter_type = 'acute inpatient'