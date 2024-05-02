


with value_set_member_relevant_fields as (
select 
  aa.concept_id,
  aa.concept_name,
  aa.concept_type,
  
  bb.value_set_member_id,
  bb.code,
  bb.coding_system_id,
  bb.include_descendants,

  cc.coding_system_name
  
from {{ ref('clinical_concepts') }} aa

left join {{ ref('value_set_members') }} bb
on aa.concept_id = bb.concept_id

left join {{ ref('coding_systems') }} cc
on bb.coding_system_id = cc.coding_system_id
)


select *
from value_set_member_relevant_fields




