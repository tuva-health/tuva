{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT distinct
    year
  , month
  , year_month
  , year_month_int
  , first_day_of_month
FROM {{ ref('semantic_layer__dim_date') }} cal