{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with combine_line_models as (
  {{ dbt_utils.union_relations(
    relations=[
      ref('service_category__pharmacy_institutional'),
      ref('service_category__outpatient_radiology_institutional'),
      ref('service_category__observation_institutional'),
      ref('service_category__ambulance_institutional'),
      ref('service_category__dme_institutional'),
      ref('service_category__lab_institutional')
    ],
    exclude=["_loaded_at"]
  ) }}
)

select
  l.claim_id
  , l.claim_line_number
  , l.service_category_1
  , l.service_category_2
  , l.service_category_3
  , l.source_model_name
from combine_line_models as l
