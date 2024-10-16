{{ config(
    enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
) }}

select
    '# of CMS chronic conditions not found in data' as data_quality_check
   , count(condition) as result_count
   , '{{ var('tuva_last_run')}}' as tuva_last_run   
from {{ ref('data_quality__chronic_conditions_missing') }}