{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(m.dispensing_date,cast('1900-01-01' as date)) as source_date
    , 'MEDICATION' as table_name
    , 'Medication ID' as drill_down_key
    , coalesce(medication_id, 'NULL') as drill_down_value
    , 'PRESCRIBING_DATE' as field_name
    , case
        when m.prescribing_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'invalid'
        when m.prescribing_date <= cast('1901-01-01' as date) then 'invalid'
        when m.prescribing_date > m.dispensing_date then 'invalid'
        when m.prescribing_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.prescribing_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'future'
        when m.prescribing_date <= cast('1901-01-01' as date) then 'too old'
        when m.prescribing_date > m.dispensing_date then 'Prescribing date after dispensing date'
        else null
    end as invalid_reason
    , cast(prescribing_date as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('medication') }} as m
