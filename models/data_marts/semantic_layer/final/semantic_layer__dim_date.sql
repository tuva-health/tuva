{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

WITH min_max_dates AS (
SELECT MIN(claim_start_date) as min_date, MAX(claim_start_date) as end_date FROM {{ ref('semantic_layer__stg_core__medical_claim') }} cal
)

SELECT distinct
    cal.full_date
  , cal.year
  , cal.month
  , cal.day
  , cal.month_name
  , cal.day_of_week_number
  , cal.day_of_week_name
  , cal.week_of_year
  , cal.day_of_year
  , cal.year_month
  , cal.first_day_of_month
  , cal.last_day_of_month
  , cal.year_month_int
  , cal.tuva_last_run
FROM {{ ref('semantic_layer__stg_reference_data__calendar') }} as cal
INNER JOIN min_max_dates as mmd
  ON cal.full_date BETWEEN mmd.min_date AND mmd.end_date
