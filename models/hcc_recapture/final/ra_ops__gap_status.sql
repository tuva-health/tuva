-- Using distinct to remove the hierarchy group and data source
select distinct
      person_id
    , payer
    , hcc_code
    , risk_model_code
    , model_version
    , payment_year
    , recapture_flag
    , gap_status
    , suspect_hcc_flag
from {{ ref('ra_ops__int_gap_status')}}
-- Apply hierarchies
where filtered_out_by_hierarchy = 0