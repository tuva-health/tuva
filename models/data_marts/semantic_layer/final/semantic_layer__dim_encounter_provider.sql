{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

with claim_provider_data as (
    select
        c.encounter_id
      , c.rendering_id
      , p.specialty
      , sum(c.paid_amount) as paid_amount
      , max(c.tuva_last_run) as tuva_last_run
  from {{ ref('semantic_layer__stg_core__medical_claim') }} as c
    inner join {{ ref('semantic_layer__dim_data_source') }} as ds on c.data_source = ds.data_source
    left join {{ ref('semantic_layer__stg_core__practitioner') }} as p
        on c.rendering_id = p.npi
    group BY
        c.encounter_id
        , c.rendering_id
        , p.specialty
),
rank_ordered as (
    select
        encounter_id
      , rendering_id
      , specialty
      , paid_amount
      , tuva_last_run
      , row_number() over (partition by encounter_id order by paid_amount desc) as rn
    from claim_provider_data
)

SELECT
    encounter_id
  , rendering_id as primary_provider_id
  , specialty
  , tuva_last_run
from rank_ordered
where rn = 1