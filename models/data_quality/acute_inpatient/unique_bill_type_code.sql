{{ config(
    enabled = var('claims_enabled', False)
) }}

with list_of_claims as (

    select distinct
          claim_id
    from {{ ref('valid_bill_type_code_counts') }}
    where valid_bill_type_code_count = 1

)

select
      claim_id
    , bill_type_code
from {{ ref('how_often_each_bill_type_code_occurs') }}
where claim_id in (
    select
          claim_id
    from list_of_claims
)
  and ranking = 1

