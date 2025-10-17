/*
This model is to identify all assignable beneficiaries who were not assigned.
This is so ACO providers can meet with these unassigned beneficiaries in order to
make them eligible for assignment.
*/

select 
      asgnbl.aco_id
    , asgnbl.performance_year
    , asgnbl.person_id
    , asgnbl.assignment_methodology
    , asgnbl.voluntarily_aligned
from {{ref('cms_provider_attribution__assignable_beneficiaries')}} asgnbl
left join {{ref('cms_provider_attribution__assigned_beneficiaries')}} asgnd
  on  asgnbl.aco_id = asgnd.aco_id
  and asgnbl.performance_year = asgnd.performance_year
  and asgnbl.person_id = asgnd.person_id
where asgnd.person_id is null -- Only those who were not assigned