{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

/*
  claim_id is only unique within a data_source (the input-layer primary key is
  claim_id + claim_line_number + data_source), so any join keyed on claim_id
  alone can pull claim lines from a second source into an encounter. An
  encounter that spans more than one data_source is always that bug, never a
  legitimate grouping.
*/

select
    encounter_id
  , count(distinct data_source) as data_source_count
from {{ ref('encounters__combined_claim_line_crosswalk') }}
group by encounter_id
having count(distinct data_source) > 1

union all

select
    encounter_id
  , count(distinct data_source) as data_source_count
from {{ ref('encounters__orphaned_claims') }}
group by encounter_id
having count(distinct data_source) > 1
