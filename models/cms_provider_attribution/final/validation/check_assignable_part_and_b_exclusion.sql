-- Confirms that the part A and B exclusions are correct
-- To run this validation, set your ALR to the final AALR for a prior performance year and update the performance year in your dbt_project.yml
-- TODO: Limit to prospective since excluded is not a field for retrospective ACOs
with excluded as (
select distinct 
    bene.person_id
from {{ref('cms_provider_attribution__stg_beneficiary_demographics')}} bene
left join {{ref('cms_provider_attribution__int_table1_a__part_a_and_b')}} ab
    on  bene.aco_id = ab.aco_id
    and bene.person_id = ab.person_id
    and bene.performance_year = ab.performance_year
where ab.person_id is null
)

select 
      bene.aco_id
    , bene.performance_year
    , count(distinct case when part_a_b_only_excluded = 1 and excl.person_id is not null then bene.person_id else null end) as correctly_excluded
    , count(distinct case when part_a_b_only_excluded = 0 and excl.person_id is null then bene.person_id else null end) as correctly_included
    , count(distinct case when part_a_b_only_excluded = 0 and excl.person_id is not null then bene.person_id else null end) as incorrectly_excluded
    , count(distinct case when part_a_b_only_excluded = 1 and excl.person_id is null then bene.person_id else null end) as incorrectly_included
from {{ref('cms_provider_attribution__stg_beneficiary_demographics')}} bene
inner join {{ref('cms_provider_attribution__stg_aalr1')}} aalr -- limit to just prospectively assigned benes
    on  bene.aco_id = aalr.aco_id
    and bene.person_id = aalr.person_id
    and bene.performance_year = aalr.performance_year 
left join excluded excl
    on aalr.person_id = excl.person_id
group by 
      bene.aco_id
    , bene.performance_year