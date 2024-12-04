{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      claim_id
    , count(distinct ms_drg_code) as valid_ms_drg_code_count
from {{ ref('valid_values') }}
where valid_ms_drg_code = 1
group by
      claim_id
