{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
      m.data_source
    {% if target.type == 'bigquery' %}
        , coalesce({{ dbt.current_timestamp() }}, cast('1900-01-01' as timestamp)) as source_date
    {% else %}
        , coalesce({{ dbt.current_timestamp() }}, cast('1900-01-01' as date)) as source_date
    {% endif %}
    , 'PATIENT' AS table_name
    , 'Patient ID' as drill_down_key
    , coalesce(patient_id, 'NULL') AS drill_down_value
    , 'RACE' as field_name
    , case when term.description is not null then 'valid'
           when m.race is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.race is not null and term.description is null
           then 'Race description does not join to Terminology race table'
           else null end as invalid_reason
    , cast(race as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('patient')}} m
left join {{ ref('terminology__race')}} term on m.race = term.description
