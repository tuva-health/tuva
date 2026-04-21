{{ config(
     enabled = var('hcc_recapture_suspect_list') | as_bool
   )
}}

select
    person_id
    , payer
    , data_source
    , recorded_date
    , model_version
    , claim_id
    , hcc_code
    , hcc_description
    , suspect_hcc_flag
    , eligible_claim_flag
    , hcc_type
    , hcc_source
from {{ ref('suspect_hccs')}}
