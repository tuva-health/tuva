{{ config(
    enabled = var('clinical_enabled', False)
) }}

SELECT
    m.data_source
    ,coalesce(m.recorded_date,cast('1900-01-01' as date)) as source_date
    ,'CONDITION' AS table_name
    ,'Condition ID' as drill_down_key
    , coalesce(condition_id, 'NULL') AS drill_down_value
    ,'PRESENT_ON_ADMIT_CODE' AS field_name
    ,case when term.present_on_admit_code is not null then 'valid'
          when m.present_on_admit_code is not null then 'invalid'
          else 'null'
    end as bucket_name
    ,case when m.present_on_admit_code is not null and term.present_on_admit_code is null
          then 'Present On Admit Code does not join to Terminology present_on_admission table'
          else null
    end as invalid_reason
    ,cast(m.present_on_admit_code as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('condition')}} m
left join {{ ref('terminology__present_on_admission')}} term on m.present_on_admit_code = term.present_on_admit_code
