/* 
Determine all eligible beneficiaries for claims-based assignment.

The AALR provides fields neeeded to determine eligibility for prospective assignment, but not retrospective assignment.
Therefore, this file uses the AALR to determine assignment eligibility.

Instructions can be found in the CMS SHARED SAVINGS AND LOSSES, ASSIGNMENT AND QUALITY PERFORMANCE STANDARD METHODOLOGY (SLAAM) for a given year.
All of the page references for the SLAAM below are referring to the 2025 SLAAM here: https://www.cms.gov/files/document/medicare-shared-savings-program-shared-savings-and-losses-and-assignment-methodology-specifications.pdf-4
This code should be reviewed yearly to ensure it stays up to date with the yearly SLAAM releases.

This model is based on pages 12-13 of the SLAAM.
*/

/* All of the instructions for determining eligible beneficiaries are summarized into the 'EXCLUDED' column,
which makes this model simple.
*/

select distinct
      bene.aco_id
    , bene.performance_year
    , bene.person_id
    , coalesce(asgn.assignment_methodology, 'prospective') as assignment_methodology
    , case when vol.person_id is not null then 1 else 0 end as voluntarily_aligned
from {{ref('cms_provider_attribution__stg_beneficiary_demographics')}} bene
inner join {{ref('cms_provider_attribution__stg_aalr1')}} aalr -- limit to just prospectively assigned benes
    on  bene.aco_id = aalr.aco_id
    and bene.person_id = aalr.person_id
    and bene.performance_year = aalr.performance_year 
/* The following joins are based on p.18 of the SLAAM under the section "2.3.2.2 Prospective Assignment"*/
inner join {{ref('cms_provider_attribution__int_table1_a__part_a_and_b')}} ab
    on  bene.aco_id = ab.aco_id
    and bene.person_id = ab.person_id
    and bene.performance_year = ab.performance_year
-- TODO: Add in private health plan enrollment if cms_provider_attribution__int_table1_b__private_health_plan has been completed
inner join {{ref('cms_provider_attribution__int_table1_d__lived_in_us')}} us
    on  bene.aco_id = us.aco_id
    and bene.person_id = us.person_id
    and bene.performance_year = us.performance_year
left join {{ref('cms_provider_attribution__int_table1_g__voluntary_alignment')}} vol
    on  bene.aco_id = vol.aco_id
    and bene.person_id = vol.person_id
    and bene.performance_year = vol.performance_year
left join {{ref('cms_provider_attribution__stg_assignment_methodology')}} asgn
    on  aalr.aco_id = asgn.aco_id
    and aalr.performance_year = asgn.performance_year
    and asgn.assignment_methodology = 'prospective'
left join {{ref('cms_provider_attribution__int_deceased_beneficiaries')}} dead
    on  bene.aco_id = dead.aco_id
    and bene.person_id = dead.person_id    
where 1=1
    and aalr.excluded = 0
    and dead.person_id is null
    and (asgn.aco_id is not null 
            -- Voluntary alignment is treated as prospective and has the exclusions applied
            /*
            2.2 Voluntary Alignment (p.14 of the SLAAM)
            "Notwithstanding the claimsbased assignment methodology (refer to Section 2.3), beneficiaries who designate an ACO
            professional participating in an ACO as responsible for coordinating their overall care are
            PROSPECTIVELY assigned to that ACO, regardless of track (and regardless of the ACO’s selection of
            beneficiary assignment methodology), annually at the beginning of each benchmark and
            performance year based on data available at the time that assignment lists are determined for the
            benchmark and performance year."
            */            
            or vol.person_id is not null)
