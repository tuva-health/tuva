{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    {% if target.type == 'bigquery' %}
        , cast(coalesce({{ dbt.current_timestamp() }}, cast('1900-01-01' as timestamp)) as date) as source_date
    {% else %}
        , cast(coalesce({{ dbt.current_timestamp() }}, cast('1900-01-01' as date)) as date) as source_date
    {% endif %}
    , 'PATIENT' as table_name
    , 'Person ID' as drill_down_key
    , coalesce(person_id, 'NULL') as drill_down_value
    , 'DEATH_DATE' as field_name
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
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('patient') }} as m
