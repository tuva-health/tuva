{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
) }}

with all_conditions as (
    select distinct
        condition
    from {{ ref('chronic_conditions__cms_chronic_conditions_hierarchy') }}
)

select
    ac.condition
from all_conditions ac
    left join {{ ref('chronic_conditions__cms_chronic_conditions_long') }} as cccl
        on ac.condition = cccl.condition
where
    cccl.condition is null