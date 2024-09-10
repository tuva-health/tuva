{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a 'refills'
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] refills_is_positive_integer ('Yes' or 'No')


with all_rows as (
select
  claim_id,
  refills
from {{ ref('pharmacy_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),


refills_is_positive_integer as (
select
  claim_id,
  case 
    when (refills = cast(refills as int) ) and
         (refills > 0) then 'Yes' 
    else 'No' 
  end as refills_is_positive_integer
from all_rows
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'Yes'
    when bb.claim_id is null then 'No'
  end as refills_is_positive_integer

from all_claim_ids aa

left join refills_is_positive_integer bb
on aa.claim_id = bb.claim_id
)


select *
from final_table
