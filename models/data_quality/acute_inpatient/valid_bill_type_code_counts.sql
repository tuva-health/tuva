{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      claim_id
    , count(distinct bill_type_code) as valid_bill_type_code_count
from {{ ref('valid_values') }}
where valid_bill_type_code = 1
group by
      claim_id
