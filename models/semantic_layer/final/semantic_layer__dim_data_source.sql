{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

with data_sources as (
SELECT DISTINCT 
  data_source
FROM {{ ref('semantic_layer__stg_core__eligibility')}}

UNION ALL

SELECT DISTINCT 
  data_source
FROM {{ ref('semantic_layer__stg_core__medical_claim')}}

UNION ALL

SELECT DISTINCT 
  data_source
FROM {{ ref('semantic_layer__stg_core__pharmacy_claim')}}

)

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['data_source']) }} as data_source_sk
  , data_source
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from data_sources