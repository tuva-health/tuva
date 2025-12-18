{{ config(
    enabled = var('clinical_enabled', False)
) }}

select
      m.data_source
    , coalesce(m.occurrence_date,cast('1900-01-01' as date)) as source_date
    , 'IMMUNIZATION' as table_name
    , 'Immunization ID' as drill_down_key
    , coalesce(immunization_id, 'NULL') as drill_down_value
    , 'OCCURRENCE_DATE' as field_name
    , case 
        when m.occurrence_date > cast(substring('{{ var('tuva_last_run') }}', 1, 10) as date) then 'invalid' 
        when m.occurrence_date <= cast('1900-01-01' as date) then 'invalid' 
        when m.occurrence_date is null then 'null'
        else 'valid' 
    end as bucket_name
    , case 
        when m.occurrence_date > cast(substring('{{ var('tuva_last_run') }}', 1, 10) as date) then 'future'
        when m.occurrence_date <= cast('1900-01-01' as date) then 'too old'
        else null 
    end as invalid_reason
    , cast(occurrence_date as {{ dbt.type_string() }}) as field_value
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('immunization') }} as m
