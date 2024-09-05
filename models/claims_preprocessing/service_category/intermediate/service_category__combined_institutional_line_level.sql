{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

WITH combine_line_models AS (
  {{ dbt_utils.union_relations(
    relations=[
      ref('service_category__pharmacy_institutional'),
      ref('service_category__outpatient_radiology_institutional'),
      ref('service_category__observation_institutional'),
      ref('service_category__lab_institutional')
    ],
    exclude=["_loaded_at"]
  ) }}
)

SELECT
  l.claim_id,
  l.claim_line_number,
  l.service_category_1,
  l.service_category_2,
  l.service_category_3,
  l.source_model_name
FROM combine_line_models l
