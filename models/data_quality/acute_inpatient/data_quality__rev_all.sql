{{ config(
    enabled = var('claims_enabled', False)
) }}

with all_rev_codes as (
      select
            claim_id
          , revenue_center_code
          , valid_revenue_center_code
      from {{ ref('data_quality__valid_values') }} bb
      where revenue_center_code is not null
),

all_distinct_revenue_center_codes_for_each_claim as (
      select distinct
            claim_id
          , revenue_center_code
          , valid_revenue_center_code
      from all_rev_codes
)

select 
      claim_id
    , revenue_center_code
    , valid_revenue_center_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from all_distinct_revenue_center_codes_for_each_claim
