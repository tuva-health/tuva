{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
      m.data_source
    , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    , 'PROCEDURE' AS table_name
    , 'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') AS drill_down_value
    , 'PROCEDURE_DATE' as field_name
    , case
        when m.procedure_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'invalid'
        when m.procedure_date <= cast('1901-01-01' as date) then 'invalid'
        when m.procedure_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.procedure_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'future'
        when m.procedure_date <= cast('1901-01-01' as date) then 'too old'
        else null
    end as invalid_reason
    , cast(procedure_date as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('procedure')}} m
