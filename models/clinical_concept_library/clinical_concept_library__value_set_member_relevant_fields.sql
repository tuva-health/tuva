


with value_set_member_relevant_fields as (
select
  aa.concept_id
  , aa.concept_name
  , aa.concept_type

  , bb.value_set_member_id
  , bb.code
  , bb.coding_system_id
  , bb.include_descendants

  , cc.coding_system_name

from {{ ref('clinical_concept_library__clinical_concepts') }} as aa

left outer join {{ ref('clinical_concept_library__value_set_members') }} as bb
on aa.concept_id = bb.concept_id

left outer join {{ ref('clinical_concept_library__coding_systems') }} as cc
on bb.coding_system_id = cc.coding_system_id
)


select *
from value_set_member_relevant_fields
