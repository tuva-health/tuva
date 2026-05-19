{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

-- Flattening months to 1 person per year
select distinct 
  person_id
  , collection_year
  , payer
from {{ ref('hcc_recapture__stg_eligible_benes') }}
-- Null age groups leads to null risk model codes. 
-- Risk model codes needs not to be null since it's used as a join argument to get equivalent coefficients.
where age_group is not null