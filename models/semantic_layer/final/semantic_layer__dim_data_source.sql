{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

with data_sources as (
SELECT DISTINCT 
  data_source
FROM {{ ref('core__eligibility')}}

UNION ALL

SELECT DISTINCT 
  data_source
FROM {{ ref('core__medical_claim')}}

UNION ALL

SELECT DISTINCT 
  data_source
FROM {{ ref('core__pharmacy_claim')}}

)

SELECT DISTINCT
    data_source
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from data_sources