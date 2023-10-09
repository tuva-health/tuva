{{ config(
     enabled = var('ccsr_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

select *, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__condition') }}