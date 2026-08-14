{{ config(
     enabled = var('clinical_enabled', False) | as_bool
   )
}}
select *
from {{ ref('encounter') }}
