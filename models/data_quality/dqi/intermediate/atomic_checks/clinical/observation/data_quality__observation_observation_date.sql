{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(m.observation_date,cast('1900-01-01' as date)) as source_date
    , 'OBSERVATION' as table_name
    , 'Observation ID' as drill_down_key
    , coalesce(observation_id, 'NULL') as drill_down_value
    , 'OBSERVATION_DATE' as field_name
    , case
        when m.observation_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'invalid'
        when m.observation_date <= cast('1901-01-01' as date) then 'invalid'
        when m.observation_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.observation_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'future'
        when m.observation_date <= cast('1901-01-01' as date) then 'too old'
        else null
    end as invalid_reason
    , cast(observation_date as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('observation') }} as m
