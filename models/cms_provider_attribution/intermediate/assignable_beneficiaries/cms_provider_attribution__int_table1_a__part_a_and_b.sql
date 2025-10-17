
/* Part A and B Enrollment: Section 2.1, Table 1, A

"Beneficiary must have at least 1 month of Part A and Part B
enrollment and cannot have any months of Part A only or Part B
only enrollment."
*/

-- Identify those with any months of part A or B only
with part_a_b_only_excluded as (
select distinct 
      performance_year
    , aco_id
    , person_id
from {{ref('cms_provider_attribution__stg_beneficiary_demographics')}}
-- Not including the runout files was found after validating excluded benes
where 1=1
  and bene_entlmt_buyin_ind in ('1', '2', 'A', 'B') 
  and runout_file = 0
)

-- Identify those with at least 1 month of part A and B
, part_a_and_b as (
select distinct 
      performance_year
    , aco_id
    , person_id
from {{ref('cms_provider_attribution__stg_beneficiary_demographics')}}
where 1=1
-- Not including the runout files was found after validating excluded benes
-- Including members with null buyin_ind was found after validating excluded benes
  and bene_entlmt_buyin_ind in ('3', 'C') or bene_entlmt_buyin_ind is null
  and runout_file = 0
)

-- Remove benes with any months of part A or B only
select 
      ab.performance_year
    , ab.aco_id
    , ab.person_id
from part_a_and_b ab
left join part_a_b_only_excluded excl
  on  ab.performance_year = excl.performance_year
  and ab.aco_id = excl.aco_id
  and ab.person_id = excl.person_id
where excl.person_id is null