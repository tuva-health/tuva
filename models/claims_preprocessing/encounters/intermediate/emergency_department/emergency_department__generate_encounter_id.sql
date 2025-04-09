{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}


select
    patient_data_source_id
  , claim_id
  , start_date
  , end_date
  , discharge_disposition_code
  , facility_id
  , row_number() over (partition by encounter_id
order by start_date, end_date, claim_id) as encounter_claim_number
  , row_number() over (partition by encounter_id
order by start_date desc, end_date desc, claim_id desc) as encounter_claim_number_desc
  , close_flag
  , min_closing_row
  , dense_rank() over (
order by encounter_id) as encounter_id
from {{ ref('emergency_department__generate_encounter_id_pre_sort') }}
