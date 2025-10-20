with cclf_denied as (
select 
    * 
from {{ref('cms_provider_attribution__stg_part_b_professional_claims')}}
where (upper(trim(clm_prcsg_ind_cd)) not in ('A','O','S','R')) or clm_carr_pmt_dnl_cd = '0'
)

, provider_type as (
select distinct
      claim_id
    , claim_line_number
    , person_id
    , case 
            when lower(asgn.primary_care_physician_step1) = 'yes' and asgn.physician = 1 then 'pcp'
            when lower(asgn.specialist_physician_step_2) = 'yes' and asgn.physician = 1 then 'specialist'
            when asgn.physician = 0 then 'npp'
        end as provider_type_for_assignment
from {{ ref('cms_provider_attribution__stg_part_b_professional_claims')}} claim
left join {{ref('cms_provider_attribution__stg_provider_specialty_assignment_codes')}} as asgn
    on right('00' + claim.clm_prvdr_spclty_cd,2) = asgn.specialty_code
)

-- NOTE: On rare occassion, a record has more than 1 valid provider for a given claim and it needs to be ranked
, provider_type_ranked as (
select 
      claim_id
    , claim_line_number
    , person_id
    , provider_type_for_assignment
    , rank() over (partition by claim_id, claim_line_number, person_id order by (
                        case 
                            when provider_type_for_assignment = 'pcp' then 3 
                            when provider_type_for_assignment = 'specialist' then 2 
                            when provider_type_for_assignment = 'npp' then 1 
                        end) asc) as provider_type_rank
from provider_type
where provider_type_for_assignment is not null
)

, add_provider_type as (
select
      svc.* 
    , coalesce(
        case 
            when fqhc_rhc_flag = 1 then 'pcp'
            else asgn.provider_type_for_assignment
        end
        , prov.provider_type_for_assignment) as provider_type_for_assignment         
    -- Outpatient is determined for inclusion based on the CCN, so the NPI does not have to be in the ACO, but the CCN does
    , case 
        when prov_supp.npi is not null then 1 
        else prov.aco_professional 
      end as aco_professional
from {{ref('cms_provider_attribution__int_table1_e__primary_care_services')}} svc
left join {{ ref('cms_provider_attribution__stg_providers') }}  as prov
    on svc.npi = prov.npi
left join provider_type_ranked asgn
    on  svc.claim_id = asgn.claim_id
    and svc.claim_line_number = asgn.claim_line_number
    and svc.person_id = asgn.person_id    
    and asgn.provider_type_rank = 1
    and outpatient_flag = 0
left join cclf_denied
    on  svc.claim_id = cclf_denied.claim_id
    and svc.claim_line_number = cclf_denied.claim_line_number
    and svc.person_id = cclf_denied.person_id    
left join (select distinct performance_year, npi from {{ref('cms_provider_attribution__stg_provider_supplier_list')}}) prov_supp
    on  svc.npi = prov_supp.npi
    and svc.performance_year = prov_supp.performance_year
where 1=1 
    and cclf_denied.claim_id is null
    and abs(allowed_amount) > 0
)

, base as (
select * from add_provider_type
where provider_type_for_assignment is not null    
)

-- Sometimes there will be inconsistent provider types across claims for a beneficiary. This resolves it, 
-- otherwise there are primary key issues when rolling up claims
, count_provider_type as (
select 
      person_id -- Including person ID to prevent changing too much from the original claims (i.e. allowing NPI type disagreement between benes, if any)
    , npi 
    , provider_type_for_assignment
    , count(*) as ct
from base
group by 
      person_id
    , npi
    , provider_type_for_assignment
)

-- Only pick the most common provider type if there is disagreement across claims for a beneficiary
, add_row_num_for_provider_type as (
select 
      person_id
    , npi 
    , provider_type_for_assignment
    , row_number() over (partition by person_id, npi order by ct desc) as row_num
from count_provider_type    
)

select
      base.performance_year
    , base.aco_id
    , base.person_id
    , base.claim_id
    , base.claim_line_number
    , base.claim_start_date
    , base.paid_date
    , base.hcpcs_code    
    , base.npi
    , base.tin
    , base.ccn
    , base.allowed_amount
    , base.fqhc_rhc_flag
    , base.method_ii_cah_flag
    , base.eta_flag
    , base.outpatient_flag
    , typ.provider_type_for_assignment
    , base.aco_professional
from base
left join (select distinct aco_id, performance_year, tin, npi from {{ref('cms_provider_attribution__stg_provider_supplier_list')}}) tin_npi_list
    on  base.performance_year = tin_npi_list.performance_year
    and base.aco_id = tin_npi_list.aco_id
    and base.tin = tin_npi_list.tin
    and base.npi = tin_npi_list.npi
left join (select distinct aco_id, performance_year, tin from {{ref('cms_provider_attribution__stg_provider_supplier_list')}}) tin_list    
    on  base.performance_year = tin_list.performance_year
    and base.aco_id = tin_list.aco_id
    and base.tin = tin_list.tin
left join {{ref('cms_provider_attribution__stg_ccn_participant_list')}} ccn_list    
    on  base.performance_year = ccn_list.performance_year
    and base.aco_id = ccn_list.aco_id
    and base.ccn = ccn_list.ccn
inner join add_row_num_for_provider_type typ
    on  base.person_id = typ.person_id
    and base.npi = typ.npi
    and typ.row_num = 1
where 1 = (case 
                -- Remove all other CCN not in the ACO
                when ccn_list.ccn is null and aco_professional = 1 and base.ccn is not null then 0 
                -- Remove aco professionals from consideration for other ACOs
                when tin_list.tin is null and aco_professional = 1 and base.tin is not null then 0 
                -- Remove aco professionals + TINs from consideration for other ACOs
                when tin_npi_list.tin is null and aco_professional = 1 and base.tin is not null then 0 
             else 1 end)
