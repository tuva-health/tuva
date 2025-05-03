{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(m.result_date,cast('1900-01-01' as date)) as source_date
    , 'LAB_RESULT' as table_name
    , 'Lab Result ID' as drill_down_key
    , coalesce(lab_result_id, 'NULL') as drill_down_value
    , 'NORMALIZED_CODE_TYPE' as field_name
    , case when term.code_type is not null then 'valid'
          when m.normalized_code_type is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.normalized_code_type is not null and term.code_type is null
           then 'Normalized code type does not join to Terminology code_type table'
           else null end as invalid_reason
    , cast(normalized_code_type as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('lab_result') }} as m
left outer join {{ ref('reference_data__code_type') }} as term on m.normalized_code_type = term.code_type
