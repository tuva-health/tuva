
-- This dbt model lists all claims that meet at least one of the 3
-- requirements that may be used to classify a claim as an acute
-- inpatient claim (the DRG requirement, the bill type requirement, and
-- the room & board requirement). That is, this dbt model lists the union
-- of all distinct claims that are in one of these 3 previous models:
-- 'bill', 'drg', 'rb_claims'. For each claim, there are flags that
-- indicate what requirements that claim meets.
-- It has these columns:
--   rb (0 or 1 flag, indicates if the claim meets the
--       room & board requirement: at least one Room & Board rev code)
--   drg (0 or 1 flag, indicates if the claim meets the DRG
--        requirement: has at least one valid MD-DRG or APR-DRG)
--   bill (0 or 1 flag, indicates if the claim meets the bill type
--         requirement: has at least one bill type starting with '11' or '12')
--   rb_drg (0 or 1 flag, indicates if the claim meets both the room & board
--           and the DRG requirements)
--   rb_bill (0 or 1 flag, indicates if the claim meets both the room & board
--            and the bill type requirement)
--   drg_bill (0 or 1 flag, indicates if the claim meets both the DRG and
--             the bill type requirements)
--   rb_drg_bill (0 or 1 flag, indicates if the claim meets the room & board,
--                the DRG, and the bill type requirements)



with rb as (
select distinct claim_id
from {{ ref('rb_claims') }}
-- Here it is important to use Tuva Logic
-- to only get R&B claims with basic = 1:
where basic = 1
-- where has_a_valid_rev_code = 1
),

drg as (
select distinct claim_id
from {{ ref('drg') }}
),

bill as (
select distinct claim_id
from {{ ref('bill') }}
),


rb_drg as (
select distinct rb.claim_id
from rb inner join drg on rb.claim_id = drg.claim_id
),

rb_bill as (
select distinct rb.claim_id
from rb inner join bill on rb.claim_id = bill.claim_id
),

drg_bill as (
select distinct drg.claim_id
from drg inner join bill on drg.claim_id = bill.claim_id
),

rb_drg_bill as (
select distinct rb.claim_id
from rb inner join drg on rb.claim_id = drg.claim_id
inner join bill on rb.claim_id = bill.claim_id
),


all_claims_in_all_above_cohorts as (
select *
from rb union all

select *
from drg union all

select *
from bill union all

select *
from rb_drg union all

select *
from rb_bill union all

select *
from drg_bill union all

select *
from rb_drg_bill
),


all_distinct_claims as (
select distinct claim_id
from all_claims_in_all_above_cohorts
)



select
  aa.claim_id,
  case when rb.claim_id is not null then 1 else 0 end as rb,
  case when drg.claim_id is not null then 1 else 0 end as drg,
  case when bill.claim_id is not null then 1 else 0 end as bill,
  case when rb_drg.claim_id is not null then 1 else 0 end as rb_drg,
  case when rb_bill.claim_id is not null then 1 else 0 end as rb_bill,
  case when drg_bill.claim_id is not null then 1 else 0 end as drg_bill,
  case when rb_drg_bill.claim_id is not null then 1 else 0 end as rb_drg_bill
  
from all_distinct_claims aa

left join rb on aa.claim_id = rb.claim_id
left join drg on aa.claim_id = drg.claim_id
left join bill on aa.claim_id = bill.claim_id
left join rb_drg on aa.claim_id = rb_drg.claim_id
left join rb_bill on aa.claim_id = rb_bill.claim_id
left join drg_bill on aa.claim_id = drg_bill.claim_id
left join rb_drg_bill on aa.claim_id = rb_drg_bill.claim_id
