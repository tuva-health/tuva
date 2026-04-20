{{ config(
    enabled = (var('enable_legacy_data_quality', false) | as_bool) and 
              (var('clinical_enabled', false) | as_bool)
    )
}}

select
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' as table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') as drill_down_value
    , 'FACILITY_ID' as field_name
    , case when term.npi is not null then 'valid'
          when m.facility_npi is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.facility_npi is not null and term.npi is null
          then 'Facility NPI does not join to Terminology provider table'
          else null end as invalid_reason
    , cast(facility_npi as {{ dbt.type_string() }}) as field_value
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('encounter') }} as m
left outer join {{ ref('provider_data__provider') }} as term on m.facility_npi = term.npi
