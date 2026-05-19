{{ config(
    enabled = (var('enable_legacy_data_quality', false) | as_bool) and 
              (var('clinical_enabled', false) | as_bool)
    )
}}

select
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' as table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') as drill_down_value
    , 'PRIMARY_DIAGNOSIS_DESCRIPTION' as field_name
    , cast(null as {{ dbt.type_string() }}) as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast(null as {{ dbt.type_string() }}) as field_value
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('encounter') }} as m
where 1 = 0
