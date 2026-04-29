with base as (
select distinct
    person_id,
    payer,
    payment_year,
    model_version,
    hcc_hierarchy_group,
    hcc_hierarchy_group_rank
from {{ ref('hcc_recapture__int_gap_status') }}
where hcc_hierarchy_group != 'no hierarchy'
    and filtered_by_hierarchy_flag = 0
)

select
    person_id,
    payer,
    payment_year,
    model_version,
    hcc_hierarchy_group,
    count(*)
from base
group by all
having count(*) > 1
