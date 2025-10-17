/*
Plurality: Section 2.1, Table 1, F
"Beneficiary must have received the plurality of their primary care
services from the participating ACO."

Section 2.3.3, p.19 states, "CMS uses allowed charges for primary care services for determining assignment."

To determine plurality, the combined allowed charges for NPIs in the provider supplier list will be compared against any other TIN/NPI (since we don't know
other ACO TIN participants). If they are greater, then the beneficiary is included; otherwise, they are excluded. 
This is a shortcoming of not knowing other ACO provider supplier lists - we are over-including by comparing only against any other TIN/NPI. 
The alternative would be to over-exclude by combining all other TIN/NPIs compared to each ACO provider NPI.

SLAAM 2.3.1, p.16 mentions the participant list and the use of CCNs to identify outpatient claims

"In combination, we use TINs identified from the certified ACO Participant List and CCNs for
Method II CAHs, ETA hospitals, FQHCs, and RHCs sourced from PECOS, as the basis for
beneficiary assignment used in program operations"

We don't have other ACO TIN assignment, so we can either:
1. Over-assign by comparing our ACO to any other TIN/CCN. 
2. Under-assign by comparing our ACO vs all other TINs combined.

We will go with option 1 to avoid missing beneficiaries who might otherwise be assigned.
*/

with identify_aco_group as (
select 
      all_steps.performance_year
    , all_steps.person_id
    , all_steps.step
    , all_steps.aco_professional
    , case 
        when prov.npi is not null then prov.aco_id
        when tin_list.tin is not null then tin_list.aco_id
        when ccn_list.ccn is not null then ccn_list.aco_id
        when all_steps.tin is not null then all_steps.tin
        when all_steps.ccn is not null then all_steps.ccn
        when all_steps.npi is not null then all_steps.npi
      end as plurality_group
    /*
    2.3.3 ASSIGNMENT ALGORITHM, p. 20 of the SLAAM
    "...from the same type of providers at any other Shared Savings Program
ACO, non-ACO CCN, or non-ACO individual or group TIN during the applicable assignment
window"
    */
    , case 
        when prov.npi is not null then 'ACO'
        when tin_list.tin is not null then 'ACO'
        when ccn_list.ccn is not null then 'ACO'
        when all_steps.tin is not null then 'TIN'
        when all_steps.ccn is not null then 'CCN'
        when all_steps.npi is not null then 'NPI'
      end as plurality_group_label
    , allowed_amount
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__all_steps')}} all_steps
left join {{ref('cms_provider_attribution__stg_tin_participant_list')}} tin_list
    on  all_steps.performance_year = tin_list.performance_year
    and all_steps.aco_id = tin_list.aco_id
    and all_steps.tin = tin_list.tin
left join {{ ref('cms_provider_attribution__stg_providers') }} prov
    on all_steps.npi = prov.npi
    and prov.aco_professional = 1
left join {{ref('cms_provider_attribution__stg_ccn_participant_list')}} ccn_list
    on  all_steps.performance_year = ccn_list.performance_year
    and all_steps.aco_id = ccn_list.aco_id
    and all_steps.ccn = ccn_list.ccn
)

, aggregate_plurality_groups as (
select
      performance_year
    , person_id
    , aco_professional
    , plurality_group
    , plurality_group_label    
    , step
    , sum(allowed_amount) as allowed_amount
from identify_aco_group
group by
      performance_year
    , person_id
    , aco_professional
    , plurality_group
    , plurality_group_label
    , step
)

select 
      performance_year
    , person_id
    , plurality_group_label
    , plurality_group
    , aco_professional
    , step
    , allowed_amount
    -- TODO: Resolve ties at the ACO level (put back to rank)
    , row_number() over (partition by performance_year, person_id order by step, allowed_amount desc) as plurality_group_rank
from aggregate_plurality_groups
