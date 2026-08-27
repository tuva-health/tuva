{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

  select distinct
      claim_id
    , data_source
  from {{ ref('encounters__stg_medical_claim') }}
  where
    service_category_2 in ('urgent care') --both inst and prof anchor
