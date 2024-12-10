{{ config(
    enabled = var('claims_enabled', False)
) }}

select distinct 
    claim_id 
from {{ ref('data_quality__header_values') }}
where 
    usable_ms_drg_code = 1 
    or usable_apr_drg_code = 1
