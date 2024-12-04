{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      claim_id
    , count(distinct admit_source_code) as valid_admit_source_code_count
from {{ ref('valid_values') }}
where valid_admit_source_code = 1
group by
      claim_id
