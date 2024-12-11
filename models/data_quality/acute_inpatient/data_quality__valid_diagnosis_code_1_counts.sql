{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      claim_id
    , count(distinct diagnosis_code_1) as valid_diagnosis_code_1_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__valid_values') }}
where valid_diagnosis_code_1 = 1
group by claim_id
