{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(cast(m.collection_datetime as {{ dbt.type_timestamp() }} ),cast('1900-01-01 00:00:00' as {{ dbt.type_timestamp() }} )) as source_date
    , 'LAB_RESULT' as table_name
    , 'Lab Result ID' as drill_down_key
    , coalesce(lab_result_id, 'NULL') as drill_down_value
    , 'COLLECTION_DATETIME' as field_name
    , case
        when cast(m.collection_datetime as {{ dbt.type_timestamp() }} ) > cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }} ) then 'invalid'
        when cast(m.collection_datetime as {{ dbt.type_timestamp() }} ) <= cast('1900-01-01 00:00:00' as {{ dbt.type_timestamp() }} ) then 'invalid'
        when m.collection_datetime > m.result_datetime then 'invalid'
        when m.collection_datetime is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when cast(m.collection_datetime as {{ dbt.type_timestamp() }} ) > cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }} ) then 'future'
        when cast(m.collection_datetime as {{ dbt.type_timestamp() }} ) <= cast('1900-01-01 00:00:00' as {{ dbt.type_timestamp() }} ) then 'too old'
        when m.collection_datetime > m.result_datetime then 'Collection date after result date'
        else null
    end as invalid_reason
    , cast(collection_datetime as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('lab_result') }} as m
