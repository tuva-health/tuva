{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

WITH min_max_dates AS (
SELECT MIN(claim_start_date) as min_date, MAX(claim_start_date) as end_date FROM {{ ref('core__medical_claim') }} cal
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
  , '{{ var('tuva_last_run') }}' as tuva_last_run
FROM {{ ref('reference_data__calendar') }} cal
INNER JOIN min_max_dates mmd
  ON cal.full_date BETWEEN mmd.min_date AND mmd.end_date
