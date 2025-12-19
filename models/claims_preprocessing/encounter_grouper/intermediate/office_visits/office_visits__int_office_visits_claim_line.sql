{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with rank_cte as (
select *
from {{ ref('office_visits__int_office_visits_union') }}
)

, crosswalk_cte as (
select old_encounter_id
, encounter_type
from {{ ref('office_visits__int_office_visits_encounter_ranking') }}
where relative_rank = 1
)

select r.claim_id
, r.claim_line_number
, r.old_encounter_id
, x.encounter_type
from rank_cte as r
inner join crosswalk_cte as x on r.old_encounter_id = x.old_encounter_id
