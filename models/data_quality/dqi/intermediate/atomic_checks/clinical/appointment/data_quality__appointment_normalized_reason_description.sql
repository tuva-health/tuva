{{ config(
    enabled = var('clinical_enabled', False)
) }}

select
      data_source
    , coalesce(cast(start_datetime as date),cast('1900-01-01' as date)) as source_date
    , 'APPOINTMENT' as table_name
    , 'Appointment ID' as drill_down_key
    , coalesce(appointment_id, 'NULL') as drill_down_value
    , 'NORMALIZED_REASON_DESCRIPTION' as field_name
    , case when normalized_reason_description is not null then 'valid' else 'null' end as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast(normalized_reason_description as {{ dbt.type_string() }}) as field_value
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('appointment') }}
