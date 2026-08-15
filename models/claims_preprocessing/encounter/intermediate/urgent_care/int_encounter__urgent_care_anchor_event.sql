{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

  select distinct
      claim_id
  from {{ ref('int_encounter__claim_line') }}
  where
    service_category_2 in ('urgent care') --both inst and prof anchor
