{{ config(
    enabled = var('claims_enabled', False)
) }}

with list_of_claims as (

    select distinct
          claim_id
    from {{ ref('data_quality__valid_drg_code_counts') }}
    where valid_drg_code_count = 1

)

select
      hc.claim_id
    , hc.drg_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__how_often_each_drg_code_occurs') }} hc
inner join list_of_claims lc 
  on hc.claim_id = lc.claim_id
    and hc.ranking = 1
