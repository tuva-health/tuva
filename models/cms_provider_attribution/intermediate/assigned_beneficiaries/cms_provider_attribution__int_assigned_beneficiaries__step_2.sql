/* SLAAM 2.3.3 - Step 2

Step 2: If not assigned in Step 1, a beneficiary received the plurality of primary care
services from specialist physicians in the participating ACO.
*/


select 
      clms.aco_id
    , clms.performance_year
    , clms.person_id
    , clms.tin
    , clms.ccn
    , clms.npi
    , clms.provider_type_for_assignment
    , clms.aco_professional
    , 2 as step
    , max(clms.claim_start_date) as max_claim_start_date
    , sum(clms.allowed_amount) as allowed_amount
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__asgn_windows')}} as clms
inner join  {{ref('cms_provider_attribution__int_assigned_beneficiaries__step_0')}} as prestep
  on  clms.aco_id = prestep.aco_id
  and clms.performance_year = prestep.performance_year
  and clms.person_id = prestep.person_id
  and clms.aco_professional = prestep.aco_professional
left join {{ref('cms_provider_attribution__int_assigned_beneficiaries__step_1')}} step1
  on  clms.aco_id = step1.aco_id
  and clms.performance_year = step1.performance_year
  and clms.person_id = step1.person_id
  and clms.aco_professional = step1.aco_professional
where 1=1
    and clms.in_rolling_12_window = 1
    and clms.provider_type_for_assignment = 'specialist'
    and step1.person_id is null
group by 
      clms.aco_id
    , clms.performance_year
    , clms.person_id
    , clms.tin
    , clms.ccn
    , clms.npi
    , clms.provider_type_for_assignment
    , clms.aco_professional
