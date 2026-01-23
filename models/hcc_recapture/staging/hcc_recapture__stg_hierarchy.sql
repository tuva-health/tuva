{{ config(
     enabled = var('hcc_recapture_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))) | as_bool
}}

with recursive hierarchy {% if target.type == 'redshift' %} (
      hcc_code
    , hccs_to_exclude
    , root_hcc
    , hcc_hierarchy_group
    , model_version
    , hcc_hierarchy_group_rank
    , path
) {% endif %} as (
    -- Base case: Start with root nodes (hcc_code with no parents)
    select
          hcc_code
        , hccs_to_exclude
        , hcc_code as root_hcc
        , description as hcc_hierarchy_group
        , model_version
        , 1 as hcc_hierarchy_group_rank
        , cast(hcc_code as {{ dbt.type_string() }}) as path
    from {{ ref('cms_hcc__disease_hierarchy') }}
    where hcc_code not in (
        select distinct hccs_to_exclude
        from {{ ref('cms_hcc__disease_hierarchy') }}
        where hccs_to_exclude is not null
    )

    union all

    -- Recursive case: Find children of current nodes
    select
        t.hcc_code as hcc_code
        , t.hccs_to_exclude
        , h.root_hcc
        , h.hcc_hierarchy_group
        , h.model_version
        , h.hcc_hierarchy_group_rank + 1 as hcc_hierarchy_group_rank
        , h.path || ' -> ' || t.hcc_code as path
    from {{ ref('cms_hcc__disease_hierarchy') }} as t
    inner join hierarchy as h
        on t.hcc_code = h.hccs_to_exclude
        and t.model_version = h.model_version
    where h.hcc_hierarchy_group_rank < 100  -- Prevent infinite loops
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
select
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
from hierarchy as hier
inner join max_group_rank as grp
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
