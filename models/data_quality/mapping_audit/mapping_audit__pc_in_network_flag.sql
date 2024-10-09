{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a in_network_flag
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] in_network_flag_unique ('Yes' or 'No')
--    [b] in_network_flag_valid_values ('Yes' or 'No')
--        (Valid values for in_network_flag are 0 or 1.


with all_rows as (
select
  claim_id,
  in_network_flag
from {{ ref('pharmacy_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),

in_network_flag_not_unique as (
select
  claim_id,
  count(distinct in_network_flag) as in_network_flag_count
from all_rows
group by claim_id
having in_network_flag_count > 1
),

in_network_flag_invalid_values as (
select distinct claim_id
from all_rows
where in_network_flag not in (0,1)
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as in_network_flag_unique,

  case
    when cc.claim_id is not null then 'No'
    when cc.claim_id is null then 'Yes'
  end as in_network_flag_valid_values

from all_claim_ids aa

left join in_network_flag_not_unique bb
on aa.claim_id = bb.claim_id

left join in_network_flag_invalid_values cc
on aa.claim_id = cc.claim_id
)


select *
from final_table
