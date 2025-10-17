/* SLAAM 2.3.3 - Step 1

Step 3: If not identified by the “pre-step”, a beneficiary received the plurality of primary
care services during the expanded window for assignment from ACO professionals.
*/

with non_pre_step_benes as (
select 
      asgn.aco_id
    , asgn.performance_year
    , asgn.person_id
    , asgn.in_rolling_12_window
    , asgn.in_rolling_24_window
    , asgn.provider_type_for_assignment
    , asgn.aco_professional
    , asgn.fqhc_rhc_flag
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__asgn_windows')}}  asgn
left join  {{ref('cms_provider_attribution__int_assigned_beneficiaries__step_0')}} prestep
  on  asgn.aco_id = prestep.aco_id
  and asgn.performance_year = prestep.performance_year
  and asgn.person_id = prestep.person_id
  and asgn.aco_professional = prestep.aco_professional
-- 'Not identified by the pre-step' is interpreted as not meeting with an ACO professional & PCP in the 12 month assignment window
where prestep.person_id is null
)

, npp_visit as (
select distinct 
      aco_id
    , performance_year
    , person_id
    , aco_professional
from non_pre_step_benes
where 1=1
    /* p.20 SLAAM, "Received at least one primary care service with a non-physician ACO professional (NP,
                    PA, or CNS) in the ACO during the applicable 12-month assignment window." */
  and in_rolling_12_window = 1 
  and ((provider_type_for_assignment = 'npp')
    )
)

, pcp_visit as (
select distinct 
      aco_id
    , performance_year
    , person_id
    , aco_professional
from non_pre_step_benes
where 1=1
    /* p. 21 SLAAM, " Received at least one primary care service with a physician who is an ACO professional
                      in the ACO 
                        and who is a primary care physician as defined under § 425.20 
                        OR 
                        who has
                        one of the primary specialty designations included in § 425.402(c) during the applicable
                        24-month expanded window for assignment." */
    and in_rolling_24_window = 1 
    and ((provider_type_for_assignment in ('pcp', 'specialist')))
) 

select 
      asgn.aco_id
    , asgn.performance_year
    , asgn.person_id
    , asgn.tin
    , asgn.ccn
    , asgn.npi
    , asgn.provider_type_for_assignment
    , asgn.aco_professional
    , 3 as step
    , max(asgn.claim_start_date) as max_claim_start_date
    , sum(allowed_amount) as allowed_amount
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__asgn_windows')}} asgn
inner join npp_visit npp
  on  asgn.aco_id = npp.aco_id
  and asgn.performance_year = npp.performance_year
  and asgn.person_id = npp.person_id
  and asgn.aco_professional = npp.aco_professional
inner join pcp_visit pcp
  on  asgn.aco_id = pcp.aco_id
  and asgn.performance_year = pcp.performance_year
  and asgn.person_id = pcp.person_id  
  and asgn.aco_professional = pcp.aco_professional
/* p.21 SLAAM states, "CMS will assign beneficiaries meeting these criteria to an ACO if the allowed charges for
primary care service furnished to the beneficiary by ACO professionals in the ACO who are
primary care physicians, non-physician ACO professionals, or physicians with specialty
designations included in § 425.402(c) during the applicable expanded window for assignment..."
*/
where 1=1
  and asgn.provider_type_for_assignment in ('pcp', 'specialist', 'npp')
group by 
      asgn.aco_id
    , asgn.performance_year
    , asgn.person_id
    , asgn.tin
    , asgn.ccn
    , asgn.npi
    , asgn.provider_type_for_assignment
    , asgn.aco_professional
