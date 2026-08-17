{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

/*
  A medical claim line is unique only within its data source. This invariant
  catches joins that accidentally match a claim line to service-category rows
  from another source.
*/

select
    claim_id
  , claim_line_number
  , data_source
  , count(*) as row_count
from {{ ref('encounters__stg_medical_claim') }}
group by
    claim_id
  , claim_line_number
  , data_source
having count(*) > 1
