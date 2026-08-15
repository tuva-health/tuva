{{ config(
     enabled = (var('clinical_enabled', False) | string | lower) == 'true'
   )
}}
select *
from {{ ref('patient') }}
