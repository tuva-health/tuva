-- Runs summary accuracy statistics compared to the ALR
-- To run this validation, set your ALR to the final AALR for a prior performance year and update the performance year in your dbt_project.yml
-- TODO: Limit to prospective since excluded is not a field for retrospective ACOs

{{ config(
     enabled = var('cms_provider_attribution_validation_enabled'False)
 | as_bool
   )
}}

with excluded_benes as (
select distinct 
    excl.mbi as person_id 
from {{ref('input_layer__cclf_bnex')}} excl
inner join (select distinct file_date from {{ref('cms_provider_attribution__int_assigned_beneficiaries__asgn_windows')}} where in_rolling_12_window = 1) asgn
    on  excl.performance_year = year(file_date)
    and excl.report_month = month(file_date)
)

, totals as 
(
select 
      assignment_type
    , count(distinct alr.person_id) as total_ct
from {{ref('cms_provider_attribution__stg_alr1')}} alr
left join excluded_benes excl
    on alr.person_id = excl.person_id
where 1=1
    -- and alr.excluded = 0
    and excl.person_id is null
group by 
    assignment_type
)

, lost_due_to_plurality as (
select distinct
    plrl.person_id
from {{ref('cms_provider_attribution__int_table1_f__aco_plurality')}} plrl
left join (select distinct person_id from {{ref('cms_provider_attribution__int_table1_f__aco_plurality')}}) prov
  on plrl.person_id = prov.person_id
where prov.person_id is null
)

, drilldown_base as (
select 
        2 as level
    , case 
        when asgn.step is null and alr.assignment_type = 0 then 'Voluntary Alignment - Unassigned'
        when alr.assignment_type = 0 then 'Voluntary Alignment - Assigned'
        when asgn.step is null then 'Unassigned'
        when asgn.step = alr.assignment_type then 'Matched'
        when asgn.step != alr.assignment_type then 'Mismatched'
      end as label
    , alr.assignment_type as correct_step
    , case when alr.assignment_type = 0 then NULL else asgn.step end as step
    , alr.person_id
    , total_ct
from  {{ref('cms_provider_attribution__stg_alr1')}} alr
full outer join {{ref('cms_provider_attribution__assigned_beneficiaries')}} asgn
    on alr.person_id = asgn.person_id
inner join totals
    on alr.assignment_type = totals.assignment_type
left join excluded_benes excl
    on alr.person_id = excl.person_id
where 1=1
    -- and excluded = 0
    and excl.person_id is null
)



, drilldown as (
select 
      drill.level
    , label
    , correct_step
    , step
    , count(distinct drill.person_id) as ct
    , avg(total_ct) as total_ct
    -- , count(distinct drill.person_id) * 1.0 / avg(total_ct)  as ratio
from drilldown_base drill
group by 
      drill.level
    , label    
    , correct_step
    , step
)

, assigned_benes as (
select count(distinct asgn.person_id) as ct from {{ref('cms_provider_attribution__assigned_beneficiaries')}} asgn
left join excluded_benes excl
    on asgn.person_id = excl.person_id
where excl.person_id is null
)
, assignable_benes as (
select count(distinct asgn.person_id) as ct from {{ref('cms_provider_attribution__assignable_beneficiaries')}} asgn
left join excluded_benes excl
    on asgn.person_id = excl.person_id
where excl.person_id is null
)

, alr_assigned as (
select count(distinct alr.person_id) as ct from {{ref('cms_provider_attribution__stg_alr1')}} alr
left join excluded_benes excl
    on alr.person_id = excl.person_id
where 1=1
    and excl.person_id is null
    -- and excluded = 0
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
    -- and excluded = 0
    and assignment_type = 2
left join excluded_benes excl
    on pot.person_id = excl.person_id
where 1=1
    and excl.person_id is null
    and pot.step = 1
)

, misassigned_from_pcp as (
select asgn.person_id, max(case when asgn.provider_type_for_assignment = 'pcp' then 1 else 0 end) as pcp_present  
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__asgn_windows')}} as asgn
inner join step2_assigned_step1 asgn_inc
    on asgn.person_id = asgn_inc.person_id
group by asgn.person_id
)

, unassigned as (
select distinct
    alr.person_id
    , alr.assignment_type as correct_step
from   {{ref('cms_provider_attribution__stg_alr1')}} alr
full outer join {{ref('cms_provider_attribution__assigned_beneficiaries')}} asgn
    on alr.person_id = asgn.person_id
left join excluded_benes excl
    on alr.person_id = excl.person_id
where 1=1
    -- and excluded = 0
    and step is null
    and excl.person_id is null
)

-- Isolate those with claims
, claims as (
select distinct base.person_id 
from unassigned as base
inner join {{"medical_claim"}} med
    on base.person_id = med.person_id
where claim_start_date between DATEADD(YEAR,-1,GETDATE()) and GETDATE()
)


, no_claims as (
select 
      3 as level
    , case 
        when correct_step = 1 then 'Unassigned Step 1 w/no claims' 
        when correct_step = 2 then 'Unassigned Step 2 w/no claims'
      end as label
    , correct_step
    , null as step
    , sum(case when claims.person_id is null then 1 else 0 end) as ct
    , count(base.person_id) as total_ct
    -- , sum(case when claims.person_id is null then 1 else 0 end) * 1.0 / count(base.person_id) as ratio
from unassigned as base 
left join claims
    on base.person_id = claims.person_id
where correct_step != 0
group by
      case 
        when correct_step = 1 then 'Unassigned Step 1 w/no claims' 
        when correct_step = 2 then 'Unassigned Step 2 w/no claims'
      end
    , correct_step
)

, plurality_drilldown as (
select 
      3 as level
    , 'Unassigned - lost due to plurality' as label
    , correct_step
    , null as step
    , sum(case when plural.person_id is not null then 1 else 0 end) as ct
    , count(base.person_id) as total_ct
    -- , sum(case when claims.person_id is null then 1 else 0 end) * 1.0 / count(base.person_id) as ratio
from unassigned as base 
left join lost_due_to_plurality as plural
    on base.person_id = plural.person_id
where correct_step != 0
group by
    correct_step
)

, possible as (
select 
      2 as level
    , concat(drill.label, ' - Possible') as label
    , correct_step
    , step
    , sum(case when claims.person_id is not null and plural.person_id is null then 1 else 0 end) as ct
    , avg(total_ct) as total_ct
from drilldown_base drill
left join lost_due_to_plurality plural
    on drill.person_id = plural.person_id
left join claims
    on drill.person_id = claims.person_id
where drill.label in ('Unassigned')
group by
      concat(drill.label, ' - Possible')
    , correct_step
    , step
)

, result_base as (
select * from drilldown
union all
select 
      1 as level
    , 'Assignable' as label
    , null as correct_step
    , null as step
    , (select ct from assignable_benes) as ct
    , (select ct from alr_assigned) as total_ct
    -- , (select ct from assignable_benes) * 1.0 / (select ct from alr_assigned) as ratio
union all
select 
     1 as level
    , 'Assigned' as label
    , null as correct_step
    , null as step
    , (select ct from assigned_benes) as ct
    , (select ct from assignable_benes) as total_ct
    -- , (select ct from assigned_benes) * 1.0 / (select ct from assignable_benes) as ratio
union all
select * from step2_misassigned_w_eta
union all 
select * from no_claims
union all
select * from plurality_drilldown
union all
select * from possible
)

, result as (
select
      2 as level
    , 'Matched - Possible' as label
    , correct_step
    , case when step is null then correct_step else step end as step
    , sum(case when label = 'Unassigned - Possible' then ct * -1 else ct end) as ct
    , sum(case when label = 'Matched' then total_ct else 0 end) as total_ct
from result_base
where label in ('Matched', 'Unassigned - Possible', 'Unassigned')
group by 
      correct_step
    , case when step is null then correct_step else step end

union all

select * from result_base
)
select * from result order by correct_step, label, level, step
