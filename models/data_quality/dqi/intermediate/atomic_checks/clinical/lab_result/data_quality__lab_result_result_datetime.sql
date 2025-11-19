{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(cast(m.result_datetime as date),cast('1900-01-01' as date)) as source_date
    , 'LAB_RESULT' as table_name
    , 'Lab Result ID' as drill_down_key
    , coalesce(lab_result_id, 'NULL') as drill_down_value
    , 'RESULT_DATETIME' as field_name
    , case
        when cast(m.result_datetime as date) > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'invalid'
        when cast(m.result_datetime as date) <= cast('1900-01-01' as date) then 'invalid'
        when m.result_datetime < m.collection_datetime then 'invalid'
        when m.result_datetime is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when cast(m.result_datetime as date) > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'future'
        when cast(m.result_datetime as date) <= cast('1900-01-01' as date) then 'too old'
        when m.result_datetime < m.collection_datetime then 'Result date before collection date'
        else null
    end as invalid_reason
    , cast(result_datetime as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('lab_result') }} as m
