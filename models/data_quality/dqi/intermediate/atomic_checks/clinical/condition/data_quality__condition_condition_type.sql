{{ config(
    enabled = var('clinical_enabled', False)
) }}

select
      m.data_source
    , coalesce(m.recorded_date,cast('1900-01-01' as date)) as source_date
    , 'CONDITION' as table_name
    , 'Condition ID' as drill_down_key
    , coalesce(condition_id, 'NULL') as drill_down_value
    , 'CONDITION_TYPE' as field_name
    , case when m.condition_type is not null then 'valid' else 'null' end as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast(condition_type as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('condition') }} as m
