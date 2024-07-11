{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}

SELECT *,
       patient_id || '|' || data_source AS patient_data_source_key
FROM {{ ref('core__patient')}}