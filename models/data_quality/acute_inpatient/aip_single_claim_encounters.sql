{{ config(
    enabled = var('claims_enabled', False)
) }}

with single_claim_encounters as (
    select
          patient_id
        , encounter_id
        , count(distinct claim_id) as claim_count
    from {{ ref('aip_encounter_id') }}
    group by patient_id, encounter_id
    having claim_count = 1
),

claims_from_single_claim_encounters as (
    select
          patient_id
        , claim_id
        , encounter_id
    from {{ ref('aip_encounter_id') }}
    where encounter_id in (
        select distinct encounter_id 
        from single_claim_encounters
    )
),

get_other_claim_data_elements as (
    select
          aa.patient_id
        , aa.claim_id
        , aa.encounter_id
        , bb.merge_start_date
        , bb.merge_end_date
        , bb.ms_drg_code
        , bb.apr_drg_code
        , bb.diagnosis_code_1
        , bb.admit_type_code
        , bb.admit_source_code
        , bb.discharge_disposition_code
        , bb.facility_npi
        , bb.rendering_npi
        , bb.paid_amount
        , bb.usable_for_aip_encounter
        , bb.dq_problem
        , bb.usable_patient_id
        , bb.usable_merge_dates
        , bb.usable_diagnosis_code_1
        , bb.usable_admit_type_code
        , bb.usable_admit_source_code
        , bb.usable_discharge_disposition_code
        , bb.usable_facility_npi
        , bb.usable_rendering_npi
        , 0 as part_of_multi_claim_encounter
    from claims_from_single_claim_encounters aa
    left join {{ ref('acute_inpatient_institutional_claims') }} bb
        on aa.patient_id = bb.patient_id
        and aa.claim_id = bb.claim_id
)

select *
from get_other_claim_data_elements
