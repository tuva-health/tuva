{{ config(
     enabled = var('ccsr_enabled',var('tuva_marts_enabled',True))
   )
}}

select *, '{{ var('last_update')}}' as last_update
from {{ ref('core__procedure') }}