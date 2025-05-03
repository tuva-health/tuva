{{ config(
    enabled = var('clinical_enabled', False)
) }}

select
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' as table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') as drill_down_value
    , 'ENCOUNTER_END_DATE' as field_name
    , case
        when m.encounter_end_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'invalid'
        when m.encounter_end_date <= cast('1901-01-01' as date) then 'invalid'
        when m.encounter_end_date < m.encounter_start_date then 'invalid'
        when m.encounter_end_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.encounter_end_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) then 'future'
        when m.encounter_end_date <= cast('1901-01-01' as date) then 'too old'
        when m.encounter_end_date < m.encounter_start_date then 'Encounter end date before encounter start date'
        else null
    end as invalid_reason
    , cast(encounter_end_date as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('encounter') }} as m
