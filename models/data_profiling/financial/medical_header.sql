select
  claim_id
  , claim_type
  , member_id
  , place_of_service_code
  , bill_type_code
  , ms_drg_code
  , facility_npi
  , min(claim_start_date) as claim_start_date
  , coalesce(max(claim_end_date), max(claim_start_date)) as claim_end_date
  , sum(allowed_amount) as allowed_amount
  , sum(charge_amount) as charge_amount
  , sum(paid_amount) as paid_amount
  , sum(total_cost_amount) as total_cost_amount
 from {{ ref('medical_claim') }}
 group by claim_id, claim_type, member_id, place_of_service_code, bill_type_code, facility_npi, ms_drg_code
