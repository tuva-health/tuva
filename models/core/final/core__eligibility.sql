{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

select * from {{ ref('core__stg_claims_eligibility') }}
