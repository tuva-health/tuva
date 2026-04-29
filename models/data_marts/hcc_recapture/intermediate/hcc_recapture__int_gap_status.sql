{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

-- Need to do this for HCCs in more than 1 group such as HCC 409 in v28
with add_rankings as (
    select
        person_id
        , payer
        , hcc_code
        , suspect_hcc_flag
        , model_version
        , collection_year + 1 as payment_year
        , recapturable_flag
        , hcc_type
        , hcc_source
        , eligible_bene_flag
        , gap_status
        , risk_model_code
        , hcc_hierarchy_group
        , hcc_hierarchy_group_rank
        , case when hcc_type = 'coded' then 1 when hcc_type = 'captured' then 2 else 3 end as hcc_type_rank
        -- just used for deduping
        , case gap_status
            when 'closed - equivalent coefficient hcc in hierarchy group' then 1
            when 'closed - higher coefficient hcc in hierarchy group' then 2
            when 'closed' then 3
            when 'closed - lower coefficient hcc in hierarchy group' then 4
            when 'new' then 5 -- New comes before open since suspects will always be open, but coded will sometimes say it is new
            when 'open' then 6
        end as gap_status_rank
    from {{ ref('hcc_recapture__int_determine_gap_status') }}
)

-- Pick the best hcc type
    -- 1. Best rank
    -- 2. If tie → prefer recapture (suspect_hcc_flag = 0)
, best_hcc_type as (
    select *
    , row_number() over (
        partition by person_id, payer, hcc_code, model_version, payment_year
        order by 
            hcc_type_rank asc,
            suspect_hcc_flag asc   -- 0 preferred over 1
    ) as best_rank
    from add_rankings
)

-- Pick the best gap status
, best_gap_status as (
    select 
      * 
    from (
      select 
      *
      , min(gap_status_rank) over (partition by person_id, payer, hcc_code, model_version, payment_year) as min_gap_status_rank 
      from best_hcc_type
      where best_rank = 1
    )
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
        , min(hcc_hierarchy_group_rank) as min_hcc_hier_group_rank
    from best_gap_status
    group by
        person_id
        , payer
        , payment_year
        , model_version
        , hcc_hierarchy_group
)

-- Using distinct to deduplicate
select distinct
    bgap.person_id
    , bgap.payer
    , bgap.hcc_code
    , bgap.risk_model_code
    , bgap.model_version
    , bgap.payment_year
    , bgap.recapturable_flag
    , bgap.hcc_type
    , bgap.hcc_source
    , bgap.gap_status
    , bgap.hcc_hierarchy_group
    , bgap.hcc_hierarchy_group_rank
    , bgap.suspect_hcc_flag
    -- Apply hierarchies (i.e. if the hierarchy is not the min hierarchy, then remove it)
    , case when bgap.hcc_hierarchy_group is not null and mhier.hcc_hierarchy_group is null then 1 else 0 end as filtered_by_hierarchy_flag
from best_gap_status as bgap
left join min_open_hierarchy as mhier
    on bgap.person_id = mhier.person_id
    and bgap.payer = mhier.payer
    and bgap.payment_year = mhier.payment_year
    and bgap.model_version = mhier.model_version
    and bgap.hcc_hierarchy_group = mhier.hcc_hierarchy_group
    and bgap.hcc_hierarchy_group_rank = mhier.min_hcc_hier_group_rank
    -- Join eligible benes again here to capture new rows with open gaps
inner join {{ ref('hcc_recapture__int_eligible_benes') }} as elig
    on bgap.person_id = elig.person_id
    and bgap.payment_year = elig.collection_year + 1
    and bgap.payer = elig.payer
where 1 = (case when bgap.payment_year >= 2026 and bgap.model_version = 'CMS-HCC-V24' then 0 else 1 end)
