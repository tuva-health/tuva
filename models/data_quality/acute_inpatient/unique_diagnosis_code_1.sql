{{ config(
    enabled = var('claims_enabled', False)
) }}

with list_of_claims as (
    select distinct claim_id
    from {{ ref('valid_diagnosis_code_1_counts') }}
    where valid_diagnosis_code_1_count = 1
)

select
      claim_id
    , diagnosis_code_1
from {{ ref('how_often_each_diagnosis_code_1_occurs') }}
where claim_id in (select claim_id from list_of_claims)
  and ranking = 1
