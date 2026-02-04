{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    {% if target.type == 'bigquery' %}
        , coalesce(cast({{ dbt.current_timestamp() }} as date), date('1900-01-01')) as source_date
    {% else %}
        , coalesce(cast({{ dbt.current_timestamp() }} as date), cast('1900-01-01' as date)) as source_date
    {% endif %}
    , 'PATIENT' as table_name
    , 'Person ID' as drill_down_key
    , coalesce(person_id, 'NULL') as drill_down_value
    , 'EMAIL' as field_name
    , case when m.email is not null then 'valid' else 'null' end as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast(email as {{ dbt.type_string() }}) as field_value
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('patient') }} as m
