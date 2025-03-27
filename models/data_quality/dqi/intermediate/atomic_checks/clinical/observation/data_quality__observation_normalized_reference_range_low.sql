{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(m.observation_date,cast('1900-01-01' as date)) as source_date
    , 'OBSERVATION' as table_name
    , 'Observation ID' as drill_down_key
    , coalesce(observation_id, 'NULL') as drill_down_value
    , 'NORMALIZED_REFERENCE_RANGE_LOW' as field_name
    , case when m.normalized_reference_range_low is not null then 'valid' else 'null' end as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast(normalized_reference_range_low as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('observation') }} as m
