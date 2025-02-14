{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      claim_id
    , count(distinct bill_type_code) as valid_bill_type_code_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__valid_values') }}
where valid_bill_type_code = 1
group by
      claim_id
