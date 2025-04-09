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
    , 'SEX' as field_name
    , case when term.gender is not null then 'valid'
           when m.sex is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.sex is not null and term.gender is null
           then 'Sex does not join to Terminology gender table'
           else null end as invalid_reason
    , cast(sex as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('patient') }} as m
left outer join {{ ref('terminology__gender') }} as term on m.sex = term.gender
