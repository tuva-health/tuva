{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}
select *
from {{ ref('medical_claim') }}
