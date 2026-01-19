{{ config(
     enabled = var('hcc_recapture_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with base as (
select
    *
from {{ ref('ra_ops__int_all_hccs')}}
-- hierarchies should only be applied to eligible claims
where eligible_claim_indicator = 1
    and hcc_chronic_flag = 1
)

, min_hierarchy as (
select
      person_id
    , payer
    , collection_year
    , model_version
    , data_source
    , hcc_hierarchy_group
    , suspect_hcc_flag
    , min(hcc_hierarchy_group_rank) as min_hcc_hier_group_rank
from base
group by
      person_id
    , payer
    , collection_year
    , model_version
    , data_source
    , hcc_hierarchy_group
    , suspect_hcc_flag
)

select
      base.person_id
    , base.payer
    , base.collection_year
    , base.recorded_date
    , base.model_version
    , base.hcc_code
    , base.hcc_description
    , base.data_source
    , base.hcc_chronic_flag
    , base.claim_id
    , base.hcc_hierarchy_group
    , base.hcc_hierarchy_group_rank
    , base.risk_model_code
    , base.eligible_bene
    , base.eligible_claim_indicator
    , base.rendering_npi
    , base.reason
    , base.condition_type
    , base.suspect_hcc_flag
from base
left join min_hierarchy mhier
    on base.person_id = mhier.person_id
    and base.payer = mhier.payer
    and base.collection_year = mhier.collection_year
    and base.model_version = mhier.model_version
    and base.data_source = mhier.data_source
    and base.hcc_hierarchy_group = mhier.hcc_hierarchy_group
    and base.hcc_hierarchy_group_rank = mhier.min_hcc_hier_group_rank
    and base.suspect_hcc_flag = mhier.suspect_hcc_flag
where 1=1
    -- Apply hierarchies
    and mhier.hcc_hierarchy_group is not null
