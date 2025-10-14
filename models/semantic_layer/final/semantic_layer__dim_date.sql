{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

WITH min_max_dates AS (
SELECT MIN(claim_start_date) as min_date, MAX(claim_start_date) as end_date FROM {{ ref('core__medical_claim') }} cal
)

SELECT distinct
    *
FROM {{ ref('reference_data__calendar') }} cal
INNER JOIN min_max_dates mmd
  ON cal.full_date BETWEEN mmd.min_date AND mmd.end_date
