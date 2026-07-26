{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}


select
    patient_data_source_id
  , claim_id
  , start_date
  , end_date
  , discharge_disposition_code
  , facility_npi
  , row_number() over (partition by encounter_id
order by start_date, end_date, claim_id) as encounter_claim_number
  , row_number() over (partition by encounter_id
order by start_date desc, end_date desc, claim_id desc) as encounter_claim_number_desc
  , close_flag
  , min_closing_row
  -- Oasis fix: encounter_id here is a claim_id, which is only guaranteed unique within
  -- (claim_id, claim_line_number, data_source) -- not globally, and not even per-patient, since
  -- a single data_source spans many different patients (patient_data_source_id values). The
  -- original dense_rank(order by encounter_id) ranked on that bare, collision-prone value, so
  -- two unrelated patients sharing a data_source could be assigned the same final encounter_id.
  -- Ordering by patient_data_source_id first keeps this a single global sequence (no partition,
  -- so ranks don't restart per patient and collide in value downstream) while guaranteeing two
  -- different patients' rows never land on the same rank, since their sort tuples always differ.
  , dense_rank() over (
order by patient_data_source_id, encounter_id) as encounter_id
  , encounter_id as original_anchor_claim
from {{ ref('emergency_department__generate_encounter_id_pre_sort') }}
