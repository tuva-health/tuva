{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

select
    pce.data_source
  , pce.claim_id
  , pce.claim_line_number
  , pce.generic_available_total_opportunity
  , pce.generic_average_cost_per_unit
  , pce.brand_cost_per_unit
  , pce.generic_available
  , pce.generic_available_sk
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('pharmacy__pharmacy_claim_expanded') }} as pce