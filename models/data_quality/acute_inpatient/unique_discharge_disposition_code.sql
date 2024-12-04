{{ config(
    enabled = var('claims_enabled', False)
) }}

with list_of_claims as (

    select distinct
          claim_id
    from {{ ref('valid_discharge_disposition_code_counts') }}
    where valid_discharge_disposition_code_count = 1

)

select
      hc.claim_id
    , hc.discharge_disposition_code
from {{ ref('how_often_each_discharge_disposition_code_occurs') }} hc
inner join list_of_claims lc
  on hc.claim_id = lc.claim_id
    and hc.ranking = 1