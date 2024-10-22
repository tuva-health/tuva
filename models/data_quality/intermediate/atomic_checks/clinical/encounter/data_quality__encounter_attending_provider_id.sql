{{ config(
    enabled = var('clinical_enabled', False)
) }}

SELECT
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' AS table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') AS drill_down_value
    , 'ATTENDING_PROVIDER_ID' AS field_name
    , case when term.npi is not null then 'valid'
          when m.attending_provider_id is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.attending_provider_id is not null and term.npi is null
          then 'Attending provider ID does not join to Terminology provider table'
          else null end as invalid_reason
    , cast(attending_provider_id as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('encounter')}} m
left join {{ ref('terminology__provider')}} term on m.attending_provider_id = term.npi
