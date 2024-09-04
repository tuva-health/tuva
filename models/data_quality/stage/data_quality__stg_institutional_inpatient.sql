{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}


    SELECT *
    FROM {{ ref('medical_claim') }}
    WHERE claim_type = 'institutional'
    AND {{ substring('bill_type_code', 1, 2) }} = '11'
