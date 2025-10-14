{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

with claim_provider_data as (
    select
        c.encounter_id
      , c.rendering_id
      , p.specialty
      , sum(c.paid_amount) as paid_amount
      , row_number() over(
          partition by
              c.encounter_id
          order by sum(c.paid_amount) desc
      ) as rn
  from {{ ref('core__medical_claim') }} as c
    inner join {{ ref('semantic_layer__dim_data_source') }} ds on c.data_source = ds.data_source
    left join {{ ref('core__practitioner') }} as p
        on c.rendering_id = p.npi
    group BY
        c.encounter_id
        , c.rendering_id
        , p.specialty
)
SELECT
    encounter_id
  , rendering_id as primary_provider_id
  , specialty
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from claim_provider_data
where rn = 1