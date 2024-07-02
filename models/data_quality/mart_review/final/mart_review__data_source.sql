{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

SELECT DISTINCT data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM {{ ref('core__medical_claim')}}
