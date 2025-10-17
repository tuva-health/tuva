/* SLAAM 2.3.3 - Step 1

Step 1: Beneficiary received the plurality of primary care services from primary care
physicians, nurse practitioners, physician assistants and clinical nurse specialists in the
participating ACO.
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
    , 1 as step
    , max(clms.claim_start_date) as max_claim_start_date
    , sum(clms.allowed_amount) as allowed_amount
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__asgn_windows')}} as clms
inner join  {{ref('cms_provider_attribution__int_assigned_beneficiaries__step_0')}} as prestep
  on  clms.aco_id = prestep.aco_id
  and clms.performance_year = prestep.performance_year
  and clms.person_id = prestep.person_id
  and clms.aco_professional = prestep.aco_professional
where 1=1
    and in_rolling_12_window = 1
    and ((clms.provider_type_for_assignment in ('pcp', 'npp')))
group by 
      clms.aco_id
    , clms.performance_year
    , clms.person_id
    , clms.tin
    , clms.ccn
    , clms.npi
    , clms.provider_type_for_assignment
    , clms.aco_professional

