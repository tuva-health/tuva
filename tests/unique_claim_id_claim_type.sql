-- This test ensures only 1 claim type per claim
with distinct_combinations as (
  select distinct
    claim_id,
    claim_type
  from {{ ref('medical_claim') }}
),

duplicates as (
  select
    claim_id,
    count(*) as n_records
  from distinct_combinations
  group by claim_id
  having count(*) > 1
)

select * from duplicates