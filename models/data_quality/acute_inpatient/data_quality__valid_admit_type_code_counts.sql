{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      claim_id
    , count(distinct admit_type_code) as valid_admit_type_code_count
from {{ ref('data_quality__valid_values') }}
where valid_admit_type_code = 1
group by
      claim_id
