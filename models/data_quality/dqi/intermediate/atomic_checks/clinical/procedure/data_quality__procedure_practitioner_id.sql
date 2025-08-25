{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    , 'PROCEDURE' as table_name
    , 'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') as drill_down_value
    , 'PRACTITIONER_ID' as field_name
    , case when m.practitioner_id is not null then 'valid' else 'null' end as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast(practitioner_id as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('procedure') }} as m
