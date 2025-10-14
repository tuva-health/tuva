{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT 
    *
    , {{ dbt.concat(["person_id", "'|'", "data_source"]) }} as person_sk
FROM {{ ref('core__patient')}}