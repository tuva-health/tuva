{{ config(
     enabled = var('cms_provider_attribution_validation_enabled'False)
 | as_bool
   )
}}

select mbi from {{ref('cclf_bnex')}}