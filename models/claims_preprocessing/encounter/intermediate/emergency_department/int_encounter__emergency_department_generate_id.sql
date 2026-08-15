{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}


select
    patient_data_source_id
  , claim_id
  , start_date
  , end_date
  , discharge_disposition_code
  , facility_npi
  , row_number() over (partition by anchor_claim_id
order by start_date, end_date, claim_id) as encounter_claim_number
  , row_number() over (partition by anchor_claim_id
order by start_date desc, end_date desc, claim_id desc) as encounter_claim_number_desc
  , close_flag
  , min_closing_row
  , {{ dbt_utils.generate_surrogate_key(['anchor_claim_id']) }} as encounter_id
  , anchor_claim_id as original_anchor_claim
from {{ ref('int_encounter__emergency_department_generate_id_pre_sort') }}
