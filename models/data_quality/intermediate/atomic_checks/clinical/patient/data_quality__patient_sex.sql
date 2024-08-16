{{ config(
    enabled = var('clinical_enabled', False)
) }}


SELECT
      m.data_source
    , coalesce(current_date,cast('1900-01-01' as date)) as source_date
    , 'PATIENT' AS table_name
    , 'Patient ID' as drill_down_key
    , coalesce(patient_id, 'NULL') AS drill_down_value
    , 'SEX' as field_name
    , case when term.gender is not null then 'valid'
           when m.sex is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.sex is not null and term.gender is null
           then 'Sex does not join to Terminology gender table'
           else null end as invalid_reason
    , cast(sex as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('patient')}} m
left join {{ ref('terminology__gender')}} term on m.sex = term.gender
