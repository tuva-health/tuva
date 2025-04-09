{{ config(
    enabled = var('clinical_enabled', False)
) }}

select
      m.data_source
    , coalesce(m.recorded_date,cast('1900-01-01' as date)) as source_date
    , 'CONDITION' as table_name
    , 'Condition ID' as drill_down_key
    , coalesce(condition_id, 'NULL') as drill_down_value
    , 'RESOLVED_DATE' as field_name
    , case
        when m.resolved_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'invalid'
        when m.resolved_date <= cast('1901-01-01' as date) then 'invalid'
        when m.resolved_date < m.onset_date then 'invalid'
        when m.resolved_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.resolved_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'future'
        when m.resolved_date <= cast('1901-01-01' as date) then 'too old'
        when m.resolved_date < m.onset_date then 'Resolved date before onset date'
        else null
    end as invalid_reason
    , cast(resolved_date as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('condition') }} as m
