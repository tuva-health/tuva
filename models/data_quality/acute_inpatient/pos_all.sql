with all_pos_codes as (
      select
            aa.claim_id
          , aa.calculated_claim_type
          , bb.place_of_service_code
          , bb.valid_place_of_service_code
      from {{ ref('claim_type') }} aa
      left join {{ ref('valid_values') }} bb
        on aa.claim_id = bb.claim_id
      where bb.place_of_service_code is not null
),

all_distinct_place_of_service_codes_for_each_claim as (
      select distinct
            claim_id
          , calculated_claim_type
          , place_of_service_code
          , valid_place_of_service_code
      from all_pos_codes
)

select 
      claim_id
    , calculated_claim_type
    , place_of_service_code
    , valid_place_of_service_code
from all_distinct_place_of_service_codes_for_each_claim
