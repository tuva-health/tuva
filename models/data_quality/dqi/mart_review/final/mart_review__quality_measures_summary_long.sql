{{ config(
     enabled = var('claims_enabled', var('clinical_enabled', False))
 | as_bool
   )
}}


select *
from {{ ref('quality_measures__summary_long') }} as s
