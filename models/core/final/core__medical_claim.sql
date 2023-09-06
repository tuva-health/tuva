{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
   )
}}


select * from {{ ref('core__stg_claims_medical_claim')}}