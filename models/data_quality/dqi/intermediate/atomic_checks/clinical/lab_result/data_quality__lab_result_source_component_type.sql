{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(m.result_datetime,cast('1900-01-01 00:00:00' as {{ dbt.type_timestamp() }} )) as source_date
    , 'LAB_RESULT' as table_name
    , 'Lab Result ID' as drill_down_key
    , coalesce(lab_result_id, 'NULL') as drill_down_value
    , 'SOURCE_COMPONENT_TYPE' as field_name
    , case when m.source_component_type is not null then 'valid' else 'null' end as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast(source_component_type as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('lab_result') }} as m
