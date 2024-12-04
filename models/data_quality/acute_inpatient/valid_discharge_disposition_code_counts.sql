{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      claim_id
    , count(distinct discharge_disposition_code) as valid_discharge_disposition_code_count
from {{ ref('valid_values') }}
where valid_discharge_disposition_code = 1
group by
      claim_id
