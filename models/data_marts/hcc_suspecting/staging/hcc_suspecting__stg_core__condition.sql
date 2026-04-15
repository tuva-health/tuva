{{ config(
     enabled = var('claims_enabled', var('clinical_enabled', False)) | as_bool
   )
}}
-- NOTE: Need distinct since condition_rank is not included
select distinct
      claim_id
    , person_id
    , payer
    , recorded_date
    , condition_type
    , lower(normalized_code_type) as code_type
    , normalized_code as code
    , data_source
from {{ ref('core__condition') }}
