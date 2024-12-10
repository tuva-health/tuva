{{ config(
    enabled = var('claims_enabled', False)
) }}

with usable_aip_inst_claims as (

    select
          claim_id
        , patient_id
        , merge_start_date
        , merge_end_date
        , discharge_disposition_code
        , facility_npi
    from {{ ref('data_quality__acute_inpatient_institutional_claims') }}
    where usable_for_aip_encounter = 1

)

, check_all_overlapping_claims as (

    select
          aa.patient_id
        , aa.claim_id as claim_id_a
        , bb.claim_id as claim_id_b
        , aa.merge_start_date as merge_start_a
        , aa.merge_end_date as merge_end_a
        , bb.merge_start_date as merge_start_b
        , bb.merge_end_date as merge_end_b
        , aa.facility_npi as facility_npi_a
        , bb.facility_npi as facility_npi_b
        , case
              when (aa.facility_npi <> bb.facility_npi) then 1
              else 0
          end as different_facility_npi
        , case
              when (
                  (aa.merge_start_date between bb.merge_start_date and bb.merge_end_date)
                  or
                  (aa.merge_end_date between bb.merge_start_date and bb.merge_end_date)
                  or
                  (bb.merge_start_date between aa.merge_start_date and aa.merge_end_date)
                  or
                  (bb.merge_end_date between aa.merge_start_date and aa.merge_end_date)
              ) then 1
              else 0
          end as overlapping_flag
    from usable_aip_inst_claims aa
    inner join usable_aip_inst_claims bb
        on aa.patient_id = bb.patient_id
        and aa.claim_id < bb.claim_id

)

, select_only_overlapping_claims as (

    select
          patient_id
        , claim_id_a
        , claim_id_b
        , merge_start_a
        , merge_end_a
        , merge_start_b
        , merge_end_b
        , facility_npi_a
        , facility_npi_b
        , different_facility_npi
    from check_all_overlapping_claims
    where overlapping_flag = 1

)

, add_encounter_ids as (

    select
          orig.patient_id
        , orig.claim_id_a
        , orig.claim_id_b
        , orig.merge_start_a
        , orig.merge_end_a
        , orig.merge_start_b
        , orig.merge_end_b
        , orig.facility_npi_a
        , orig.facility_npi_b
        , orig.different_facility_npi
        , aa.encounter_id as encounter_id_a
        , bb.encounter_id as encounter_id_b
        , case
              when (aa.encounter_id <> bb.encounter_id) then 1
              else 0
          end as different_encounter_id
    from select_only_overlapping_claims orig
    left join {{ ref('data_quality__aip_encounter_id') }} aa
        on orig.patient_id = aa.patient_id
        and orig.claim_id_a = aa.claim_id
    left join {{ ref('data_quality__aip_encounter_id') }} bb
        on orig.patient_id = bb.patient_id
        and orig.claim_id_b = bb.claim_id

)

select *
from add_encounter_ids
