{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(m.dispensing_date,cast('1900-01-01' as date)) as source_date
    , 'MEDICATION' as table_name
    , 'Medication ID' as drill_down_key
    , coalesce(medication_id, 'NULL') as drill_down_value
    , 'NDC_DESCRIPTION' as field_name
    , case when term.ndc is not null then 'valid'
           when m.ndc_code is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.ndc_code is not null and term.ndc is null
           then 'NDC code type does not join to Terminology ndc table'
           else null end as invalid_reason
    , cast(substring(ndc_description, 1, 255) as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('medication') }} as m
left outer join {{ ref('terminology__ndc') }} as term on m.ndc_code = term.ndc
