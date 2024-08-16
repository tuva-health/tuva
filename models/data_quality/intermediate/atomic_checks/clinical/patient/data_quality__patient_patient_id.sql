{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
      m.data_source
    , coalesce(current_date,cast('1900-01-01' as date)) as source_date
    , 'PATIENT' AS table_name
    , 'Patient ID' as drill_down_key
    , coalesce(patient_id, 'NULL') AS drill_down_value
    , 'PATIENT_ID' as field_name
    , case when m.patient_id is not null then 'valid' else 'null' end as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast(patient_id as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('patient')}} m
