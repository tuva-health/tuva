{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

with rank_cte as (
select *
from {{ ref('int_encounter__office_visit_type_priority') }}
)

, crosswalk_cte as (
select old_encounter_id
, encounter_type
from {{ ref('int_encounter__office_visit_type_ranked') }}
where relative_rank = 1
)

select r.claim_id
, r.claim_line_number
, r.old_encounter_id
, x.encounter_type
from rank_cte as r
inner join crosswalk_cte as x on r.old_encounter_id = x.old_encounter_id
