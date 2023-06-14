{{ config(
     enabled = var('readmissions_enabled',var('tuva_marts_enabled',True))
   )
}}

select
  encounter_id
, code
, code_type
, '{{ var('last_update')}}' as last_update
from {{ ref('core__procedure') }}

