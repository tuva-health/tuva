{{ config(
    enabled = var('claims_enabled', False)
) }}

with list_of_claims as (
    select distinct claim_id
    from {{ ref('data_quality__valid_diagnosis_code_1_counts') }}
    where valid_diagnosis_code_1_count = 1
)

select
      claim_id
    , diagnosis_code_1
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__how_often_each_diagnosis_code_1_occurs') }}
where claim_id in (select claim_id from list_of_claims)
  and ranking = 1
