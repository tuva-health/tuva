with eligible_hccs as (
    select 
        * 
    from {{ ref('ra_ops__int_hccs') }} 
)

-- Get recapturable HCCs within the past 2 years
, recapturable_hccs as (
select distinct
      person_id
    , payer
    , collection_year + 1 as collection_year
    , model_version
    , hcc_code
    , hcc_hierarchy_group
    , hcc_hierarchy_group_rank
    , suspect_hcc_flag
from eligible_hccs
)

, best_past_rank as (
select distinct
       person_id
     , payer
     , collection_year
     , model_version
     , hcc_code
     , hcc_hierarchy_group
     , hcc_hierarchy_group_rank
     , min(hcc_hierarchy_group_rank) over (partition by person_id, payer, collection_year, model_version, hcc_hierarchy_group) as best_past_rank
from recapturable_hccs
-- This is only for what was actually coded in the past
where suspect_hcc_flag = 0
)


, best_current_rank as (
select distinct
       person_id
     , payer
     , collection_year
     , model_version
     , hcc_code
     , hcc_hierarchy_group
     , hcc_hierarchy_group_rank
     , min(hcc_hierarchy_group_rank) over (partition by person_id, payer, collection_year, model_version, hcc_hierarchy_group) as best_current_rank
from eligible_hccs
)

, equiv_coef as (
select distinct 
    base.model_version
  , base.hcc_hierarchy_group
  , base.hcc_code
  , base.risk_model_code
from {{ ref('ra_ops__stg_coef_hier') }} as base
inner join {{ ref('ra_ops__stg_coef_hier') }} as self
    on base.hcc_hierarchy_group = self.hcc_hierarchy_group
    and base.risk_model_code = self.risk_model_code
    and base.coefficient = self.coefficient
    and base.model_version = self.model_version
    and base.hcc_code != self.hcc_code
)

, add_gap_status as (
select
      coalesce(recap.person_id, base.person_id) as person_id
    , coalesce(recap.payer, base.payer) as payer
    , coalesce(recap.hcc_code, base.hcc_code) as hcc_code
    , coalesce(recap.suspect_hcc_flag,0) as suspect_hcc_flag
    , recap.hcc_code as recaptured_hcc_code
    , current_year_hier.hcc_code as current_year_hcc_code
    , grp.hcc_code as past_year_hcc_code
    , coalesce(recap.model_version, base.model_version) as model_version
    , coalesce(recap.collection_year, base.collection_year) as collection_year
    , coalesce(recap.hcc_hierarchy_group, base.hcc_hierarchy_group) as hcc_hierarchy_group
    , coalesce(recap.hcc_hierarchy_group_rank, base.hcc_hierarchy_group_rank) as hcc_hierarchy_group_rank
    , base.risk_model_code
    , case when recap.hcc_code is not null or grp.hcc_hierarchy_group is not null or base.hcc_chronic_flag = 1 then 1 else 0 end as recapture_flag
    , eligible_bene
    , case 
        when base.hcc_chronic_flag = 0 then 'inappropriate for recapture'
        when recap.hcc_code is not null and base.hcc_code is not null and recap.hcc_code != base.hcc_code and equiv.risk_model_code is not null then 'closed - equivalent coefficient hcc in hierarchy group'
        when grp.hcc_hierarchy_group is not null and base.hcc_hierarchy_group_rank < grp.best_past_rank then 'closed - higher coefficient hcc in hierarchy group'
        when current_year_hier.best_current_rank < recap.hcc_hierarchy_group_rank then 'closed - higher coefficient hcc in hierarchy group'
        when recap.hcc_code is not null and base.hcc_code is not null then 'closed'
        when grp.hcc_hierarchy_group is not null and base.hcc_hierarchy_group_rank > grp.best_past_rank then 'closed - lower coefficient hcc in hierarchy group'
        -- TODO: Should the current_year_hier be used to determine if something was closed by a lower coefficient hcc?
        -- This was previously removed due to errors in primary key checks it was causing
        when recap.hcc_code is not null and base.hcc_code is null then 'open'
        when recap.hcc_code is null and base.hcc_code is not null and base.hcc_chronic_flag = 1 then 'new'
      end as gap_status
from eligible_hccs as base
full outer join recapturable_hccs as recap
    on base.person_id = recap.person_id
    and base.payer = recap.payer
    and base.collection_year = recap.collection_year
    and base.model_version = recap.model_version
    and base.hcc_code = recap.hcc_code
left join equiv_coef as equiv
  on base.model_version = equiv.model_version
  and base.hcc_hierarchy_group = equiv.hcc_hierarchy_group
  and base.hcc_code = equiv.hcc_code
  and base.risk_model_code = equiv.risk_model_code
left join best_past_rank as grp
    on base.person_id = grp.person_id
    and base.payer = grp.payer
    and base.collection_year = grp.collection_year
    and base.model_version = grp.model_version
    and base.hcc_hierarchy_group = grp.hcc_hierarchy_group
    and grp.best_past_rank = grp.hcc_hierarchy_group_rank
left join best_current_rank as current_year_hier
    on recap.person_id = current_year_hier.person_id
    and recap.payer = current_year_hier.payer
    and recap.collection_year = current_year_hier.collection_year
    and recap.model_version = current_year_hier.model_version
    and recap.hcc_hierarchy_group = current_year_hier.hcc_hierarchy_group
    and current_year_hier.best_current_rank = current_year_hier.hcc_hierarchy_group_rank
-- Filtering to just discharge diagnosis since gaps are only eligible to be closed by claims data and the base table here is closing the recap aliased table
-- The or condition_type is null allows open HCCs to flow through
where lower(base.condition_type) = 'discharge_diagnosis' or base.condition_type is null
)

-- Need to do this for HCCs in more than 1 group such as HCC 409 in v28
, rank_gap_status as (
select 
      person_id
    , payer
    , hcc_code
    , suspect_hcc_flag
    , model_version
    , collection_year + 1 as payment_year
    , recapture_flag
    , eligible_bene
    , gap_status
    , risk_model_code
    , hcc_hierarchy_group
    , hcc_hierarchy_group_rank
    -- just used for deduping
    , case gap_status
        when 'inappropriate for recapture' then 1
        when 'closed - equivalent coefficient hcc in hierarchy group' then 2
        when 'closed - higher coefficient hcc in hierarchy group' then 3
        when 'closed' then 4
        when 'closed - lower coefficient hcc in hierarchy group' then 5
        when 'open' then 6
        when 'new' then 7
      end as gap_status_rank
from add_gap_status
)

, min_gap_status as (
select
      person_id
    , payer
    , hcc_code
    , suspect_hcc_flag
    , model_version
    , payment_year
    , risk_model_code
    , recapture_flag
    , eligible_bene
    , gap_status
    , gap_status_rank
    , hcc_hierarchy_group
    , hcc_hierarchy_group_rank
    , min(gap_status_rank) over (partition by person_id, payer, hcc_code, model_version, payment_year) as min_gap_status_rank
from rank_gap_status
)

-- Pick the best gap status
, best_gap_status as (
select 
  * 
from min_gap_status
where min_gap_status_rank = gap_status_rank
)

-- Find the minimum hierarchy for open hccs
, min_open_hierarchy as (
select
      person_id
    , payer
    , payment_year
    , model_version
    , hcc_hierarchy_group
    , suspect_hcc_flag
    , min(hcc_hierarchy_group_rank) as min_hcc_hier_group_rank
from best_gap_status
group by
      person_id
    , payer
    , payment_year
    , model_version
    , hcc_hierarchy_group
    , suspect_hcc_flag
)

-- Using distinct to deduplicate
select distinct
      bgap.person_id
    , bgap.payer
    , bgap.hcc_code
    , bgap.risk_model_code
    , bgap.model_version
    , bgap.payment_year
    , bgap.recapture_flag
    , bgap.gap_status
    , bgap.hcc_hierarchy_group
    , bgap.hcc_hierarchy_group_rank
    , bgap.suspect_hcc_flag
    -- Apply hierarchies (i.e. if the hierarchy is not the min hierarchy, then remove it)
    , case when bgap.hcc_hierarchy_group is not null and mhier.hcc_hierarchy_group is null then 1 else 0 end as filtered_out_by_hierarchy
from best_gap_status bgap
left join min_open_hierarchy mhier
    on bgap.person_id = mhier.person_id
    and bgap.payer = mhier.payer
    and bgap.payment_year = mhier.payment_year
    and bgap.model_version = mhier.model_version
    and bgap.hcc_hierarchy_group = mhier.hcc_hierarchy_group
    and bgap.hcc_hierarchy_group_rank = mhier.min_hcc_hier_group_rank
    and bgap.suspect_hcc_flag = mhier.suspect_hcc_flag
-- Join eligible benes again here to capture new rows with open gaps
inner join {{ ref('ra_ops__stg_eligible_benes')}} elig
  on bgap.person_id = elig.person_id
  and bgap.payment_year = elig.collection_year + 1
  and bgap.payer = elig.payer
where 1 = (case when bgap.payment_year >= 2026 and bgap.model_version = 'CMS-HCC-V24' then 0 else 1 end)