{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

-- We want to show all claims that have a place_of_service_code
-- with or without data quality problems.
-- We will have an output table at the claim_id grain and for
-- each claim_id we show:
--    [a] place_of_service_code_correct_length ('Yes' or 'No')
--    [b] place_of_service_code_unique ('Yes' or 'No')

with all_rows as (
select
  claim_id,
  place_of_service_code
from {{ ref('medical_claim') }}
where claim_id is not null
),

all_claim_ids as (
select distinct claim_id
from all_rows
),


place_of_service_code_not_correct_length as (
select
  distinct claim_id
from all_rows
where place_of_service_code is not null and
length(place_of_service_code) <> 2
),


final_table as (
select
  aa.claim_id,
  
  case
    when bb.claim_id is not null then 'No'
    when bb.claim_id is null then 'Yes'
  end as place_of_service_code_correct_length


from all_claim_ids aa

left join place_of_service_code_not_correct_length bb
on aa.claim_id = bb.claim_id
)


select *
from final_table
