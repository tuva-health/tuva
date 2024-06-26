{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',False))
   )
}}

SELECT *
, DENSE_RANK () OVER (ORDER BY INPUT_LAYER_TABLE_NAME, CLAIM_TYPE, FIELD_NAME) as TABLE_CLAIM_TYPE_FIELD_SK
FROM {{ ref('data_quality__crosswalk_field_to_mart') }}