{{ config(
     enabled = var('hcc_recapture_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

WITH RECURSIVE hierarchy AS (
    -- Base case: Start with root nodes (hcc_code with no parents)
    SELECT 
        hcc_code,
        hccs_to_exclude,
        hcc_code AS root_hcc,
        description as hcc_hierarchy_group,
        model_version,
        1 AS hcc_hierarchy_group_rank,
        CAST(hcc_code AS VARCHAR) AS path
    FROM {{ ref('cms_hcc__disease_hierarchy') }}
    WHERE hcc_code NOT IN (
        SELECT DISTINCT hccs_to_exclude 
        FROM {{ ref('cms_hcc__disease_hierarchy') }}
        WHERE hccs_to_exclude IS NOT NULL
    )
    
    UNION ALL
    
    -- Recursive case: Find children of current nodes
    SELECT 
        t.hcc_code as hcc_code,
        t.hccs_to_exclude,
        h.root_hcc,
        h.hcc_hierarchy_group,
        h.model_version,
        h.hcc_hierarchy_group_rank + 1 AS hcc_hierarchy_group_rank,
        h.path || ' -> ' || t.hcc_code AS path
    FROM {{ ref('cms_hcc__disease_hierarchy') }} as t
    INNER JOIN hierarchy as h
        ON  t.hcc_code = h.hccs_to_exclude
        and t.model_version = h.model_version
    WHERE h.hcc_hierarchy_group_rank < 100  -- Prevent infinite loops
)

, max_group_rank as (
select 
      model_version
    , hcc_hierarchy_group
    , max(hcc_hierarchy_group_rank) as max_rank
from hierarchy
group by 
      model_version
    , hcc_hierarchy_group
)

, combine_leaf_nodes as (
select distinct
      hcc_code
    , hcc_hierarchy_group
    , model_version
    , max(hcc_hierarchy_group_rank) as hcc_hierarchy_group_rank
from hierarchy
group by 
      hcc_code
    , hcc_hierarchy_group
    , model_version

union all

select distinct
      hier.hccs_to_exclude as hcc_code
    , hier.hcc_hierarchy_group
    , hier.model_version
    , grp.max_rank + 1 as hcc_hierarchy_group_rank
from hierarchy hier
inner join max_group_rank grp
    on hier.model_version = grp.model_version
    and hier.hcc_hierarchy_group = grp.hcc_hierarchy_group
    and hier.hcc_hierarchy_group_rank = grp.max_rank
)

select
      hcc_code
    , model_version
    , hcc_hierarchy_group
    , hcc_hierarchy_group_rank
from combine_leaf_nodes
