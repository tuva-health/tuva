{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      claim_id
    , count(distinct apr_drg_code) as valid_apr_drg_code_count
from {{ ref('data_quality__valid_values') }}
where valid_apr_drg_code = 1
group by
      claim_id
