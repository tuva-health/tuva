{{ config(
    enabled = var('claims_enabled', False)
) }}

with list_of_claims as (

    select distinct
          claim_id
    from {{ ref('valid_ms_drg_code_counts') }}
    where valid_ms_drg_code_count = 1

)

select
      hc.claim_id
    , hc.ms_drg_code
from {{ ref('how_often_each_ms_drg_code_occurs') }} hc
inner join list_of_claims lc 
  on hc.claim_id = lc.claim_id
    and hc.ranking = 1
