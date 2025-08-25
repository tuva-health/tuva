{{ config(
    enabled = var('clinical_enabled', False)
) }}

select
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' as table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') as drill_down_value
    , 'ENCOUNTER_TYPE' as field_name
    , case when term.encounter_type is not null then 'valid'
          when m.encounter_type is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.encounter_type is not null and term.encounter_type is null
          then 'Encounter type does not join to Terminology encounter_type table'
          else null end as invalid_reason
    , cast(m.encounter_type as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('encounter') }} as m
left outer join {{ ref('terminology__encounter_type') }} as term on m.encounter_type = term.encounter_type
