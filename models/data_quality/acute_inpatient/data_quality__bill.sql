{{ config(
    enabled = var('claims_enabled', False)
) }}

select distinct 
      claim_id 
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__header_values') }}
where 
    usable_bill_type_code = 1 
    and substring(assigned_bill_type_code, 1, 2) in ('11', '12')
