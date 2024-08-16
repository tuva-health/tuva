{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
      m.data_source
    , coalesce(current_date,cast('1900-01-01' as date)) as source_date
    , 'PATIENT' AS table_name
    , 'Patient ID' as drill_down_key
    , coalesce(patient_id, 'NULL') AS drill_down_value
    , 'DEATH_DATE' AS field_name
    , case
        when m.death_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'invalid'
        when m.death_date <= cast('1901-01-01' as date) then 'invalid'
        when m.death_date > m.birth_date then 'invalid'
        when m.death_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.death_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'future'
        when m.death_date <= cast('1901-01-01' as date) then 'too old'
        when m.death_date > m.birth_date then 'Death date after birth date'
        else null
    end as invalid_reason
    , cast(death_date as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('patient')}} m
