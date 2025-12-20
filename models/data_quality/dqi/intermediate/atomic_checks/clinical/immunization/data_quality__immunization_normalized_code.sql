{{ config(
    enabled = var('clinical_enabled', False)
) }}

select
      m.data_source
    , coalesce(m.occurrence_date,cast('1900-01-01' as date)) as source_date
    , 'IMMUNIZATION' as table_name
    , 'Immunization ID' as drill_down_key
    , coalesce(immunization_id, 'NULL') as drill_down_value
    , 'NORMALIZED_CODE' as field_name
    , case when term.cvx is not null then 'valid'
           when m.normalized_code is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when  m.normalized_code is not null and term.cvx is null
           then 'Normalized code does not join to Terminology cvx table'
           else null end as invalid_reason
    , cast(m.normalized_code as {{ dbt.type_string() }}) as field_value
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('data_quality__stg_immunization') }} as m
left outer join {{ ref('terminology__cvx') }} as term on m.normalized_code = term.cvx