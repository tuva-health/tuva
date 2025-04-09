{{ config(
    enabled = var('clinical_enabled', False)
) }}

select
    m.data_source
    ,coalesce(m.recorded_date,cast('1900-01-01' as date)) as source_date
    ,'CONDITION' as table_name
    ,'Condition ID' as drill_down_key
    , coalesce(condition_id, 'NULL') as drill_down_value
    ,'ONSET_DATE' as field_name
    ,case
        when m.onset_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'invalid'
        when m.onset_date <= cast('1901-01-01' as date) then 'invalid'
        when m.onset_date > m.resolved_date then 'invalid'
        when m.onset_date is null then 'null'
        else 'valid'
    end as bucket_name
    ,case
        when m.onset_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'future'
        when m.onset_date <= cast('1901-01-01' as date) then 'too old'
        when m.onset_date < m.resolved_date then 'Onset date after resolved date'
        else null
    end as invalid_reason
    ,cast(onset_date as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('condition') }} as m
