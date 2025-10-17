-- Combines prospective + retrospective benes

select 
      aco_id
    , performance_year
    , person_id
    , assignment_methodology
    , voluntarily_aligned
from {{ref('cms_provider_attribution__int_assignable_beneficiaries_prospective')}}

union all

select 
      aco_id
    , performance_year
    , person_id
    , assignment_methodology
    , voluntarily_aligned
from {{ref('cms_provider_attribution__int_assignable_beneficiaries_retrospective')}}