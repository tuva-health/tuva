{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

with anchor as (
    select distinct
        mc.patient_data_source_id
      , mc.start_date
      , mc.claim_id
      , mc.claim_line_number
      , mc.service_category_1
      , mc.service_category_2
      , mc.service_category_3
    from {{ ref('encounters__stg_medical_claim') }} as mc
    inner join {{ ref('service_category__combined_professional') }} as p -- joining in all sc regardless of final priority
      on mc.claim_id = p.claim_id
      and mc.claim_line_number = p.claim_line_number
    where p.service_category_1 = 'office-based'
)

select
    patient_data_source_id
  , start_date
  , claim_id
  , claim_line_number
  , service_category_1
  , service_category_2
  , service_category_3
  , dense_rank() over (
order by patient_data_source_id, start_date) as old_encounter_id
from anchor
