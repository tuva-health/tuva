{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

  select distinct
      claim_id
  from {{ ref('encounters__stg_medical_claim') }}
  where
    service_category_2 in ('urgent care') --both inst and prof anchor
