{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with rank_cte as (
select *
from {{ ref('office_visits__int_office_visits_union') }}
)

, dist_encounter as (
select distinct old_encounter_id
, encounter_type
, priority_number
from rank_cte
)

select
old_encounter_id
, encounter_type
, priority_number
, row_number() over (partition by old_encounter_id
order by priority_number) as relative_rank
from dist_encounter
