{{ config(
     enabled = var('cms_provider_attribution_validation_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

select mbi from {{ref('cclf_bnex')}}