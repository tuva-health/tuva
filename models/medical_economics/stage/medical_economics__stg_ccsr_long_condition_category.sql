{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select 
    person_id
  , claim_id 
  , body_system as condition_grouper_1
  , ccsr_category_description as condition_grouper_2 
  , code_description as condition_grouper_3
from 
    {{ ref('ccsr__long_condition_category') }} 
where condition_rank = 1
    and ccsr_category_rank = 1
