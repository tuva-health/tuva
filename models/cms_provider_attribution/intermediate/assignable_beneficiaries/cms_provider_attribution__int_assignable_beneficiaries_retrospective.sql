/*
A beneficiary is considered assignable if they meet criteria in Section 2.1, Table 1 for criteria A-E or G. 
Criteria F is determined during assignment steps 1-3.
*/
-- TODO: Add criteria B and C if able to get the necessary data sources

with distinct_primary_care_benes as (
select distinct
      prov.performance_year
    , prov.aco_id
    , prov.person_id

from {{ref('cms_provider_attribution__int_table1_e__primary_care_services_by_valid_providers')}} prov
left join {{ref('cms_provider_attribution__stg_aalr1')}} aalr
    on  prov.aco_id = aalr.aco_id
    and prov.person_id = aalr.person_id
    and prov.performance_year = aalr.performance_year 
where 1=1
    and (provider_type_for_assignment in ('pcp', 'specialist'))
    -- Only consider additions from AALR based on future claim effective dates
    and 1 = (case 
                when aalr.person_id is not null then 1
                when quarter = 1 and prov.paid_date > cast(concat(cast(prov.performance_year as varchar), '-03-31') as date) then 1
                when quarter = 2 and prov.paid_date > cast(concat(cast(prov.performance_year as varchar), '-06-30') as date) then 1
                when quarter = 3 and prov.paid_date > cast(concat(cast(prov.performance_year as varchar), '-09-30') as date) then 1
                when quarter = 4 and prov.paid_date > cast(concat(cast(prov.performance_year as varchar), '-12-31') as date) then 1
                else 0
            end
            )
)

select distinct
      bene.aco_id
    , bene.performance_year
    , bene.person_id
    , asgn.assignment_methodology
    , 0 as voluntarily_aligned
from {{ref('cms_provider_attribution__stg_beneficiary_demographics')}} bene
inner join {{ref('cms_provider_attribution__int_table1_a__part_a_and_b')}} ab -- Criteria A
    on  bene.aco_id = ab.aco_id
    and bene.performance_year = ab.performance_year
    and bene.person_id = ab.person_id
inner join {{ref('cms_provider_attribution__int_table1_d__lived_in_us')}} us -- Criteria D
    on  bene.aco_id = us.aco_id
    and bene.performance_year = us.performance_year
    and bene.person_id = us.person_id
inner join distinct_primary_care_benes pcp -- Criteria E
    on  bene.aco_id = pcp.aco_id
    and bene.performance_year = pcp.performance_year
    and bene.person_id = pcp.person_id
left join {{ref('cms_provider_attribution__int_deceased_beneficiaries')}} dead
    on  bene.aco_id = dead.aco_id
    and bene.person_id = dead.person_id
inner join {{ref('cms_provider_attribution__stg_assignment_methodology')}} asgn
    on  bene.aco_id = asgn.aco_id
    and bene.performance_year = asgn.performance_year
    and asgn.assignment_methodology = 'retrospective'
left join {{ref('cms_provider_attribution__int_table1_g__voluntary_alignment')}} vol
    on  bene.aco_id = vol.aco_id
    and bene.person_id = vol.person_id
    and bene.performance_year = vol.performance_year
where 1=1 
    and dead.person_id is null
    and vol.person_id is null