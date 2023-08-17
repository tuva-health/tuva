{{ config(
     enabled = var('readmissions_enabled',var('tuva_marts_enabled',True))
   )
}}

select
  encounter_id
, normalized_code
, normalized_code_type
, '{{ var('last_update')}}' as last_update
from {{ ref('core__procedure') }}

