
with exclude_network_providers as (
select 
  plural.*
from {{ref('cms_provider_attribution__int_table1_f__aco_plurality')}} plural
left join (select distinct tin from {{ref('cms_provider_attribution__stg_network_providers')}}) netwk_tin
  on plural.plurality_group = cast(netwk_tin.tin as varchar)
left join (select distinct ccn from {{ref('cms_provider_attribution__stg_network_providers')}}) netwk_ccn
  on plural.plurality_group = cast(netwk_ccn.ccn as varchar)
left join (select distinct npi from {{ref('cms_provider_attribution__stg_network_providers')}}) netwk_npi
  on plural.plurality_group = cast(netwk_npi.npi as varchar)
where 1=1
  and 1 = (case 
              when aco_professional = 0 and netwk_tin.tin is not null then 0
              when aco_professional = 0 and netwk_ccn.ccn is not null then 0
              when aco_professional = 0 and netwk_npi.npi is not null then 0
            else 1
          end)
)


, plurality_groups_ranked as (
select
      plural.* 
    , rank() over (partition by performance_year, person_id order by step, allowed_amount desc) as winning_plurality_group_rank
from exclude_network_providers plural
where 1=1
  -- Exclude non-ACO providers who are listed as within the ACO
  and not (plurality_group_label = 'ACO' and aco_professional = 0)
  -- Exclude ACO professionals from consideration for those outside of the ACO
  and not (plurality_group_label != 'ACO' and aco_professional = 1)
)

, aggregate_allowed_amount_by_bene_provider as (
select distinct
      steps.performance_year
    , steps.aco_id
    , steps.person_id
    , steps.step
    , steps.npi
    , coalesce(prov.aco_professional, steps.aco_professional) as aco_professional
    , sum(steps.allowed_amount) as allowed_amount
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__all_steps')}} steps
left join {{ ref('cms_provider_attribution__stg_providers') }}  as prov
  on steps.npi = prov.npi
inner join plurality_groups_ranked asgn
  on  steps.performance_year = asgn.performance_year
  and steps.person_id = asgn.person_id
  and steps.aco_id = asgn.plurality_group
  and steps.step = asgn.step
  and asgn.winning_plurality_group_rank = 1
  and asgn.plurality_group_label = 'ACO'
group by 
      steps.performance_year
    , steps.aco_id
    , steps.person_id
    , steps.step 
    , steps.npi
    , coalesce(prov.aco_professional, steps.aco_professional)
)

-- Get the latest provider visits to break ties
-- 'Tie-breaker methodology' in p.21 of the SLAAM 
, latest_pcp_npp_visit as (
select 
    person_id
  , npi
  , max(max_claim_start_date) as latest_pcp_npp_visit
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__all_steps')}}
where provider_type_for_assignment in ('pcp','npp')
group by 
    person_id
  , npi
)

, latest_specialist_visit as (
select 
    person_id
  , npi
  , max(max_claim_start_date) as latest_specialist_visit
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__all_steps')}}
where provider_type_for_assignment in ('specialist')
group by 
    person_id
  , npi
)

, numbered_providers as (
select 
      bene.performance_year
    , bene.aco_id
    , bene.person_id
    , bene.step
    , bene.npi
    , bene.aco_professional
    , ltst_pn.latest_pcp_npp_visit
    , ltst_s.latest_specialist_visit
    , bene.allowed_amount
from aggregate_allowed_amount_by_bene_provider bene
left join latest_pcp_npp_visit ltst_pn
  on  bene.person_id = ltst_pn.person_id
  and bene.npi = ltst_pn.npi
left join latest_specialist_visit ltst_s
  on  bene.person_id = ltst_s.person_id
  and bene.npi = ltst_s.npi
)

select 
      performance_year
    , aco_id
    , person_id
    , step
    , npi
    , aco_professional
    , allowed_amount
    , row_number() over (partition by performance_year, aco_id, person_id order by step, allowed_amount desc, latest_pcp_npp_visit desc, latest_specialist_visit desc) as provider_num
from numbered_providers