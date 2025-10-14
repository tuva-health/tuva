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
  , '{{ var('tuva_last_run') }}' as tuva_last_run
FROM {{ ref('semantic_layer__dim_date') }} cal