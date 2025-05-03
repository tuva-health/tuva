{{ config(
    enabled = var('clinical_enabled', False)
) }}


select
      m.data_source
    , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    , 'PROCEDURE' as table_name
    , 'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') as drill_down_value
    , 'MODIFIER_5' as field_name
    , case when term.hcpcs is not null then 'valid'
           when m.modifier_5 is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.modifier_5 is not null and term.hcpcs is null
           then 'Modifier 5 does not join to Terminology hcpcs_level_2 table'
           else null end as invalid_reason
    , cast(modifier_5 as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('procedure') }} as m
left outer join {{ ref('terminology__hcpcs_level_2') }} as term on m.modifier_5 = term.hcpcs
