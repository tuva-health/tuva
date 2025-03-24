{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      claim_id
    , count(distinct discharge_disposition_code) as valid_discharge_disposition_code_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__valid_values') }}
where valid_discharge_disposition_code = 1
group by
      claim_id
