{{ config(
    enabled = var('clinical_enabled', False)
) }}

select
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' as table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') as drill_down_value
    , 'PRIMARY_DIAGNOSIS_CODE' as field_name
    , case when term.icd_10_cm is not null then 'valid'
          when m.primary_diagnosis_code is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.primary_diagnosis_code is not null and term.icd_10_cm is null
          then 'Primary diagnosis code does not join to Terminology icd_10_cm table'
    else null end as invalid_reason
    , cast(primary_diagnosis_code as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('encounter') }} as m
left outer join {{ ref('terminology__icd_10_cm') }} as term on m.primary_diagnosis_code = term.icd_10_cm
