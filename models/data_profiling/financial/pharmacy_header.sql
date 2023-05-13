select
  claim_id
  , member_id
  , sum(allowed_amount) as allowed_amount
  , sum(paid_amount) as paid_amount
  from {{ ref('pharmacy_claim') }}
 group by claim_id, member_id
