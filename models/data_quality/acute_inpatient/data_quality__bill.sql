{{ config(
    enabled = var('claims_enabled', False)
) }}

select distinct 
    claim_id 
from {{ ref('data_quality__header_values') }}
where 
    usable_bill_type_code = 1 
    and substr(assigned_bill_type_code, 1, 2) in ('11', '12')
