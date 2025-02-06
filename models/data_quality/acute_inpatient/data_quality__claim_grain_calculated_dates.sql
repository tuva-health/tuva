{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      claim_id
    , max(merge_start_date) as merge_start_date
    , max(merge_end_date) as merge_end_date
    , max(merge_start_date_after_merge_end_date) as merge_start_date_after_merge_end_date
    , max(usable_merge_dates) as usable_merge_dates
    , max(dx_date) as dx_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__claim_dates') }}
group by 
      claim_id
