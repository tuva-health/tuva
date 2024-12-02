{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
   )
}}

select
      claim_id
    , max(merge_start_date) as merge_start_date
    , max(merge_end_date) as merge_end_date
    , max(merge_start_date_after_merge_end_date) as merge_start_date_after_merge_end_date
    , max(usable_merge_dates) as usable_merge_dates
    , max(dx_date) as dx_date
from {{ ref('claim_dates') }}
group by 
      claim_id
