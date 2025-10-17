-- Runs summary accuracy statistics compared to the ALR
-- To run this validation, set your ALR to the final AALR for a prior performance year and update the performance year in your dbt_project.yml
-- TODO: Limit to prospective since excluded is not a field for retrospective ACOs
with totals as 
(
select 
      assignment_type
    , count(distinct person_id) as total_ct
from {{ref('cms_provider_attribution__stg_alr1')}}
where 1=1
    and excluded = 0
group by 
    assignment_type
)

, drilldown as (
select 
      2 as level
      , case 
        when alr.assignment_type = 0 and asgn.step = 0 then 3
        when alr.assignment_type = 0 and asgn.step = 1 then 4
        when alr.assignment_type = 0 and asgn.step = 2 then 5
        when alr.assignment_type = 0 and asgn.step is null then 6
        when alr.assignment_type = 1 and asgn.step = 1 then 7
        when alr.assignment_type = 1 and asgn.step = 0 then 8
        when alr.assignment_type = 1 and asgn.step = 2 then 9
        when alr.assignment_type = 1 and asgn.step is null then 10
        when alr.assignment_type = 2 and asgn.step = 2 then 11
        when alr.assignment_type = 2 and asgn.step = 0 then 12 
        when alr.assignment_type = 2 and asgn.step = 1 then 13
        when alr.assignment_type = 2 and asgn.step is null then 15
      end as label_order
    , case 
        when asgn.step is null then 'Unassigned'
        when asgn.step = alr.assignment_type then 'Matched'
        when asgn.step != alr.assignment_type then 'Mismatched'
      end as label
    , alr.assignment_type as correct_step
    , asgn.step
    , count(distinct alr.person_id) as ct
    , avg(total_ct) as total_ct
    , count(distinct alr.person_id) * 1.0 / avg(total_ct)  as ratio
from  {{ref('cms_provider_attribution__stg_alr1')}} alr
full outer join {{ref('cms_provider_attribution__assigned_beneficiaries')}} asgn
    on alr.person_id = asgn.person_id
inner join totals
    on alr.assignment_type = totals.assignment_type
where 1=1
    and excluded = 0
group by alr.assignment_type, asgn.step
)
, assigned_benes as (
select count(distinct person_id) as ct from {{ref('cms_provider_attribution__assigned_beneficiaries')}}
)
, assignable_benes as (
select count(distinct person_id) as ct from {{ref('cms_provider_attribution__assignable_beneficiaries')}}
)
, alr_assigned as (
select count(distinct person_id) as ct from {{ref('cms_provider_attribution__stg_alr1')}}
where excluded = 0
)
-- Step 2 misassigned as step 1
-- What proportion of specialists don't get assignment type 2 due to a PCP?
, step2_assigned_step1 as (
select distinct
  pot.person_id
from  {{ref('cms_provider_attribution__assigned_beneficiaries')}} pot
inner join {{ref('cms_provider_attribution__stg_alr1')}} alr
    on pot.person_id = alr.person_id
    and pot.step != alr.assignment_type
    and excluded = 0
    and assignment_type = 2
where pot.step = 1
)

, misassigned_from_pcp as (
select asgn.person_id, max(case when asgn.provider_type_for_assignment = 'pcp' then 1 else 0 end) as pcp_present  
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__asgn_windows')}} as asgn
inner join step2_assigned_step1 asgn_inc
    on asgn.person_id = asgn_inc.person_id
group by asgn.person_id
)

, step2_assigned_step1_w_eta_claims as (
select distinct miss.person_id
from step2_assigned_step1 miss
inner join{{ref('cms_provider_attribution__int_assigned_beneficiaries__asgn_windows')}} as asgn
    on miss.person_id = asgn.person_id
where eta_flag = 1
)

, step2_misassigned_w_eta as (
select 
      3 as level
    , 14 as label_order
    , 'Mismatched Step 2 as Step 1 w/ETA claims' as label
    , 2 as correct_step
    , 1 as step
    , (select count(distinct person_id) from step2_assigned_step1_w_eta_claims) as ct
    , (select count(distinct person_id) from step2_assigned_step1) as total_ct
    , (select count(distinct person_id) from step2_assigned_step1_w_eta_claims) * 1.0 / (select count(distinct person_id) from step2_assigned_step1) as ratio
)
, result as (
select * from drilldown
union all
select 
      1 as level
    , 1 as label_order
    , 'Assignable' as label
    , null as correct_step
    , null as step
    , (select ct from assignable_benes) as ct
    , (select ct from alr_assigned) as total_ct
    , (select ct from assignable_benes) * 1.0 / (select ct from alr_assigned) as ratio
union all
select 
     1 as level
    , 2 as label_order
    , 'Assigned' as label
    , null as correct_step
    , null as step
    , (select ct from assigned_benes) as ct
    , (select ct from assignable_benes) as total_ct
    , (select ct from assigned_benes) * 1.0 / (select ct from assignable_benes) as ratio
union all
select * from step2_misassigned_w_eta
)
select * from result order by label_order