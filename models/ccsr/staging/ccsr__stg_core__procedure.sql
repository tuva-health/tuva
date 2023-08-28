{{ config(
     enabled = var('ccsr_enabled',var('claims_enabled',var('tuva_marts_enabled',True)))
   )
}}

select *, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__procedure') }}