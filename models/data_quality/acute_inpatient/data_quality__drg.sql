{{ config(
    enabled = var('claims_enabled', False)
) }}

select distinct 
      claim_id 
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__header_values') }}
where 
    usable_ms_drg_code = 1 
    or usable_apr_drg_code = 1
