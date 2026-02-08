{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
-- NOTE: Need distinct since condition_rank is not included
select distinct
      claim_id
    , person_id
    , payer
    , {{ quote_column('plan') }}
    , recorded_date
    , condition_type
    , lower(normalized_code_type) as code_type
    , normalized_code as code
    , data_source
from {{ ref('core__condition') }}
