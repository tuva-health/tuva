{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a claim_line_start_date
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] claim_line_start_date_always_populated ('Yes' or 'No')


with all_rows as (
select
  claim_id,
  claim_line_start_date
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),

claim_line_start_date_not_always_populated as (
select distinct claim_id
from all_rows
where claim_line_start_date is null
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as claim_line_start_date_always_populated

from all_claim_ids aa

left join claim_line_start_date_not_always_populated bb
on aa.claim_id = bb.claim_id
)


select *
from final_table
