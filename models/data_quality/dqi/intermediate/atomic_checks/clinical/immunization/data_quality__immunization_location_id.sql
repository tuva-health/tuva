{{ config(
    enabled = var('clinical_enabled', False)
) }}

select
      m.data_source
    , coalesce(m.occurrence_date,cast('1900-01-01' as date)) as source_date
    , 'IMMUNIZATION' as table_name
    , 'Immunization ID' as drill_down_key
    , coalesce(immunization_id, 'NULL') as drill_down_value
    , 'LOCATION_ID' as field_name
    , case when m.location_id is not null then 'valid' else 'null' end as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast(location_id as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__stg_immunization') }} as m
