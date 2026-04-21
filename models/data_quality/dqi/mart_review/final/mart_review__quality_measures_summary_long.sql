{{ config(
     enabled = (var('enable_legacy_data_quality', false) | as_bool)
     and (var('claims_enabled', var('clinical_enabled', False)) | as_bool)
   )
}}


select *
from {{ ref('quality_measures__summary_long') }} as s
