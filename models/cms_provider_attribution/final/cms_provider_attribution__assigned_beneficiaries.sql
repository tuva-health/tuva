-- If there is a non-ACO professional who was attributed (this can happen if there a provider bills to an ACO TIN)
-- then choose the top provider who is in the aco
with top_aco_provider as (
select 
      performance_year
    , aco_id
    , person_id
    , max(provider_num) as max_provider_num
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__provider_plurality')}}
where aco_professional = 1
group by 
      performance_year
    , aco_id
    , person_id
)

, assigned_benes as (
select 
    plural.* 
from {{ref('cms_provider_attribution__int_assigned_beneficiaries__provider_plurality')}} plural
inner join top_aco_provider prov
    on  plural.performance_year = prov.performance_year
    and plural.aco_id = prov.aco_id
    and plural.person_id = prov.person_id
    and plural.provider_num = prov.max_provider_num 
)

select 
      coalesce(plural.performance_year, vol.performance_year) as performance_year
    , coalesce(plural.aco_id, vol.aco_id) as aco_id
    , coalesce(plural.person_id, vol.person_id) as person_id
    , coalesce(plural.step, vol.step) as step
    , coalesce(vol.npi, plural.npi) as npi
    , asgn.voluntarily_aligned
from assigned_benes plural
full outer join {{ref('cms_provider_attribution__int_assignable_beneficiaries')}} asgn
    on  plural.aco_id = asgn.aco_id
    and plural.performance_year = asgn.performance_year
    and plural.person_id = asgn.person_id
    and voluntarily_aligned = 1
left join {{ref('cms_provider_attribution__int_table1_g__voluntary_alignment')}} vol
    on  asgn.aco_id = vol.aco_id
    and asgn.performance_year = vol.performance_year
    and asgn.person_id = vol.person_id
where plural.person_id is not null or vol.person_id is not null

