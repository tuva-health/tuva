{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(m.result_datetime,cast('1900-01-01 00:00:00' as {{ dbt.type_timestamp() }} )) as source_date
    , 'LAB_RESULT' as table_name
    , 'Lab Result ID' as drill_down_key
    , coalesce(lab_result_id, 'NULL') as drill_down_value
    , 'RESULT_DATETIME' as field_name
    , case
        when m.result_datetime > cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }} ) then 'invalid'
        when m.result_datetime <= cast('1900-01-01 00:00:00' as {{ dbt.type_timestamp() }} ) then 'invalid'
        when m.result_datetime < m.collection_datetime then 'invalid'
        when m.result_datetime is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.result_datetime > cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }} ) then 'future'
        when m.result_datetime <= cast('1900-01-01 00:00:00' as {{ dbt.type_timestamp() }} ) then 'too old'
        when m.result_datetime < m.collection_datetime then 'Result date before collection date'
        else null
    end as invalid_reason
    , cast(result_datetime as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('lab_result') }} as m
