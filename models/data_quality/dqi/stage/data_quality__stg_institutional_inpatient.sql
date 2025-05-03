{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}


    select *
    from {{ ref('input_layer__medical_claim') }}
    where claim_type = 'institutional'
    and {{ substring('bill_type_code', 1, 2) }} = '11'
