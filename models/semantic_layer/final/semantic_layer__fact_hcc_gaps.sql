{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT
    lr.person_id
  , lr.hcc_code
  , lr.hcc_description
  , lr.reason
  , lr.contributing_factor
  , lr.latest_suspect_date
  , lr.tuva_last_run
FROM {{ ref('hcc_suspecting__list_rollup') }} lr