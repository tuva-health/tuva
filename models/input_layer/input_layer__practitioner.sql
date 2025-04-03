{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}
SELECT *
FROM {{ ref('practitioner') }}
