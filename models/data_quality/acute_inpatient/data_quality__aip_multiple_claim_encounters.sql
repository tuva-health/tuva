{{ config(
    enabled = var('claims_enabled', False)
) }}

with multiple_claim_encounters as (
    select
          person_id
        , encounter_id
        , count(distinct claim_id) as claim_count
    from {{ ref('data_quality__aip_encounter_id') }}
    group by person_id, encounter_id
    having count(distinct claim_id) > 1
),

claims_from_multiple_claim_encounters as (
    select
          person_id
        , claim_id
        , encounter_id
    from {{ ref('data_quality__aip_encounter_id') }}
    where encounter_id in (
        select distinct encounter_id 
        from multiple_claim_encounters
    )
),

get_other_claim_data_elements as (
    select
          aa.person_id
        , aa.claim_id
        , aa.encounter_id
        , bb.merge_start_date
        , bb.merge_end_date
        , bb.drg_code
        , bb.diagnosis_code_1
        , bb.admit_type_code
        , bb.admit_source_code
        , bb.discharge_disposition_code
        , bb.facility_npi
        , bb.rendering_npi
        , bb.paid_amount
        , bb.usable_for_aip_encounter
        , bb.dq_problem
        , bb.usable_drg_code
        , bb.usable_person_id
        , bb.usable_merge_dates
        , bb.usable_diagnosis_code_1
        , bb.usable_admit_type_code
        , bb.usable_admit_source_code
        , bb.usable_discharge_disposition_code
        , bb.usable_facility_npi
        , bb.usable_rendering_npi
        , 1 as part_of_multi_claim_encounter
    from claims_from_multiple_claim_encounters aa
    left join {{ ref('data_quality__acute_inpatient_institutional_claims') }} bb
        on aa.person_id = bb.person_id
        and aa.claim_id = bb.claim_id
)

select
      person_id
    , claim_id
    , encounter_id
    , merge_start_date
    , merge_end_date
    , drg_code
    , diagnosis_code_1
    , admit_type_code
    , admit_source_code
    , discharge_disposition_code
    , facility_npi
    , rendering_npi
    , paid_amount
    , usable_for_aip_encounter
    , dq_problem
    , usable_drg_code
    , usable_person_id
    , usable_merge_dates
    , usable_diagnosis_code_1
    , usable_admit_type_code
    , usable_admit_source_code
    , usable_discharge_disposition_code
    , usable_facility_npi
    , usable_rendering_npi
    , part_of_multi_claim_encounter
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from get_other_claim_data_elements
