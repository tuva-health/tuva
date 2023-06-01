{{ config(
     enabled = var('readmissions_enabled',var('tuva_marts_enabled',True))
   )
}}

select
  encounter_id
, code
, code_type
from {{ ref('core__procedure') }}

