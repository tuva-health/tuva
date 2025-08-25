{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(m.dispensing_date,cast('1900-01-01' as date)) as source_date
    , 'MEDICATION' as table_name
    , 'Medication ID' as drill_down_key
    , coalesce(medication_id, 'NULL') as drill_down_value
    , 'PATIENT_ID' as field_name
    , case when m.patient_id is not null then 'valid' else 'null' end as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast(patient_id as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('medication') }} as m
