{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(m.result_date,cast('1900-01-01' as date)) as source_date
    , 'LAB_RESULT' as table_name
    , 'Lab Result ID' as drill_down_key
    , coalesce(lab_result_id, 'NULL') as drill_down_value
    , 'ORDERING_PRACTITIONER_ID' as field_name
    , case when term.npi is not null then 'valid'
          when m.ordering_practitioner_id is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.ordering_practitioner_id is not null and term.npi is null
           then 'Ordering practitioner ID does not join to Terminology provider table'
           else null end as invalid_reason
    , cast(ordering_practitioner_id as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('lab_result') }} as m
left outer join {{ ref('terminology__provider') }} as term on m.ordering_practitioner_id = term.npi
