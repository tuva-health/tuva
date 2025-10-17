{{ config(
     enabled = var('cms_provider_attribution_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}
select *
from {{ ref('practitioner') }}
