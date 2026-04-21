-- Get recapturable HCCs within the past 1 year
with filtered_hccs as (
    select *
    from {{ ref('hcc_recapture__int_recapturable_hccs') }}
    where filtered_by_hierarchy_flag = 0
),

risk_gaps as (
    select distinct
        person_id
        , payer
        , collection_year + 1 as collection_year
        , model_version
        , hcc_code
        , hcc_hierarchy_group
        , hcc_hierarchy_group_rank
        , suspect_hcc_flag
        , recapturable_flag
        , hcc_type
        , hcc_source
        , eligible_bene_flag
        , risk_model_code
    from filtered_hccs
    where hcc_type = 'coded'

    union all

    -- No need to add +1 to collection year since these are already identified as captured/suspect in the same year identified
    -- These come from other sources besides claims, such as payers + clinical data
    select distinct
        person_id
        , payer
        , collection_year
        , model_version
        , hcc_code
        , hcc_hierarchy_group
        , hcc_hierarchy_group_rank
        , suspect_hcc_flag
        , recapturable_flag
        , hcc_type
        , hcc_source
        , eligible_bene_flag
        , risk_model_code
    from filtered_hccs
    where hcc_type = 'suspect'
),

best_past_rank as (
    select distinct
        person_id
        , payer
        , collection_year
        , model_version
        , hcc_code
        , hcc_hierarchy_group
        , hcc_hierarchy_group_rank
        , min(hcc_hierarchy_group_rank) over (partition by person_id, payer, collection_year, model_version, hcc_hierarchy_group) as best_past_rank
    from filtered_hccs
    where hcc_type in ('coded', 'captured')
),


best_current_rank as (
    select distinct
        person_id
        , payer
        , collection_year
        , model_version
        , hcc_code
        , hcc_hierarchy_group
        , hcc_hierarchy_group_rank
        , risk_model_code
        , eligible_bene_flag
        , min(hcc_hierarchy_group_rank) over (partition by person_id, payer, collection_year, model_version, hcc_hierarchy_group) as best_current_rank
    from filtered_hccs
),

equiv_coef as (
    select distinct
        base.model_version
        , base.hcc_hierarchy_group
        , base.hcc_code
        , base.risk_model_code
    from {{ ref('hcc_recapture__stg_coef_hier') }} as base
    inner join {{ ref('hcc_recapture__stg_coef_hier') }} as self
        on
            base.hcc_hierarchy_group = self.hcc_hierarchy_group
            and base.risk_model_code = self.risk_model_code
            and base.coefficient = self.coefficient
            and base.model_version = self.model_version
            and base.hcc_code != self.hcc_code
)

-- Note: Gaps can only be closed using claims received from the payor or from Athena.
select
    coalesce(base.payer, gap.payer) as payer
    , coalesce(base.person_id, gap.person_id) as person_id
    , coalesce(base.risk_model_code, gap.risk_model_code, current_year_hier.risk_model_code) as risk_model_code
    , coalesce(base.eligible_bene_flag, gap.eligible_bene_flag, current_year_hier.eligible_bene_flag) as eligible_bene_flag
    , coalesce(base.hcc_code, gap.hcc_code) as hcc_code
    , gap.hcc_code as recaptured_hcc_code
    , current_year_hier.hcc_code as current_year_hcc_code
    , grp.hcc_code as past_year_hcc_code
    , coalesce(base.suspect_hcc_flag, gap.suspect_hcc_flag, 0) as suspect_hcc_flag
    , coalesce(base.model_version, gap.model_version) as model_version
    , coalesce(base.collection_year, gap.collection_year) as collection_year
    , coalesce(base.hcc_hierarchy_group, gap.hcc_hierarchy_group) as hcc_hierarchy_group
    , coalesce(base.hcc_hierarchy_group_rank, gap.hcc_hierarchy_group_rank) as hcc_hierarchy_group_rank
    , coalesce(base.recapturable_flag, gap.recapturable_flag) as recapturable_flag
    , coalesce(base.hcc_type, gap.hcc_type) as hcc_type
    , coalesce(base.hcc_source, gap.hcc_source) as hcc_source
    , case
        when
            gap.hcc_code is not null and base.hcc_code is not null and gap.hcc_code != base.hcc_code and equiv.risk_model_code is not null
            then 'closed - equivalent coefficient hcc in hierarchy group'
        when
            grp.hcc_hierarchy_group is not null and base.hcc_hierarchy_group_rank < grp.best_past_rank
            then 'closed - higher coefficient hcc in hierarchy group'
        when current_year_hier.best_current_rank < gap.hcc_hierarchy_group_rank then 'closed - higher coefficient hcc in hierarchy group'
        when gap.hcc_code is not null and base.hcc_code is not null then 'closed'
        when
            grp.hcc_hierarchy_group is not null and base.hcc_hierarchy_group_rank > grp.best_past_rank
            then 'closed - lower coefficient hcc in hierarchy group'
        when gap.hcc_code is not null and base.hcc_code is null then 'open'
        when gap.hcc_code is null and base.hcc_code is not null then 'new'
    end as gap_status
from filtered_hccs as base
full outer join risk_gaps as gap
    on
        base.person_id = gap.person_id
        and base.payer = gap.payer
        and base.collection_year = gap.collection_year
        and base.model_version = gap.model_version
        and base.hcc_code = gap.hcc_code
        -- Only coded or captured HCCs can close other HCCs
        and base.hcc_type in ('coded', 'captured')
left join equiv_coef as equiv
    on
        base.model_version = equiv.model_version
        and base.hcc_hierarchy_group = equiv.hcc_hierarchy_group
        and base.hcc_code = equiv.hcc_code
        and base.risk_model_code = equiv.risk_model_code
left join best_past_rank as grp
    on
        base.person_id = grp.person_id
        and base.payer = grp.payer
        and base.collection_year = grp.collection_year
        and base.model_version = grp.model_version
        and base.hcc_hierarchy_group = grp.hcc_hierarchy_group
        and grp.best_past_rank = grp.hcc_hierarchy_group_rank
left join best_current_rank as current_year_hier
    on
        gap.person_id = current_year_hier.person_id
        and gap.payer = current_year_hier.payer
        and gap.collection_year = current_year_hier.collection_year
        and gap.model_version = current_year_hier.model_version
        and gap.hcc_hierarchy_group = current_year_hier.hcc_hierarchy_group
        and current_year_hier.best_current_rank = current_year_hier.hcc_hierarchy_group_rank
-- Gaps are only eligible to be closed by claims data and the base table here is closing the gap aliased table
-- The or hcc_type is null allows open HCCs to flow through
where base.hcc_type in ('coded', 'captured') or base.hcc_type is null
