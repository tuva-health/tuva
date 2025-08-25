{{ config(
    enabled = var('claims_enabled', False)
) }}

select distinct -- to bring to claim_ID grain
    m.data_source
    ,coalesce(cast(m.claim_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'MEDICAL_CLAIM' as table_name
    ,'Claim ID' as drill_down_key
,coalesce(claim_id, 'NULL') as drill_down_value
    ,m.claim_type as claim_type
    ,'PLAN' as field_name
    ,case when m.{{ quote_column('plan') }} is not null then 'valid' else 'null' end as bucket_name
    ,cast(null as {{ dbt.type_string() }}) as invalid_reason
    ,cast({{ quote_column('plan') }} as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('medical_claim') }} as m
