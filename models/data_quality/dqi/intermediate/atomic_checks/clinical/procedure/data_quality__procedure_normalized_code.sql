{{ config(
    enabled = var('clinical_enabled', False)
) }}

with icd9 as (
    select
          m.data_source
        , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
        , 'PROCEDURE' as table_name
        , 'Procedure ID' as drill_down_key
        , coalesce(procedure_id, 'NULL') as drill_down_value
        , 'NORMALIZED_CODE' as field_name
        , case when term.icd_9_pcs is not null then 'valid'
               when m.normalized_code is not null then 'invalid'
               else 'null'
        end as bucket_name
        , case when m.normalized_code is not null and term.icd_9_pcs is null
               then 'Normalized code does not join to Terminology icd_9_pcs table'
               else null end as invalid_reason
        , cast(normalized_code as {{ dbt.type_string() }}) as field_value
    from {{ ref('procedure') }} as m
    left outer join {{ ref('terminology__icd_9_pcs') }} as term on m.normalized_code = term.icd_9_pcs
    where
        m.normalized_code_type = 'icd-9-pcs'
)
, icd10 as (
    select
      m.data_source
    , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    , 'PROCEDURE' as table_name
    , 'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') as drill_down_value
    , 'NORMALIZED_CODE' as field_name
    , case when term.icd_10_pcs is not null then 'valid'
           when m.normalized_code is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.normalized_code is not null and term.icd_10_pcs is null
           then 'Normalized code does not join to Terminology icd_10_pcs table'
           else null end as invalid_reason
    , cast(normalized_code as {{ dbt.type_string() }}) as field_value
from {{ ref('procedure') }} as m
left outer join {{ ref('terminology__icd_10_pcs') }} as term on m.normalized_code = term.icd_10_pcs
where
    m.normalized_code_type = 'icd_10_pcs'
)
, hcpcs_level_2 as (
    select
      m.data_source
    , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    , 'PROCEDURE' as table_name
    , 'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') as drill_down_value
    , 'NORMALIZED_CODE' as field_name
    , case when term.hcpcs is not null then 'valid'
           when m.normalized_code is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.normalized_code is not null and term.hcpcs is null
           then 'Normalized code does not join to Terminology hcpcs_level_2 table'
           else null end as invalid_reason
    , cast(normalized_code as {{ dbt.type_string() }}) as field_value
from {{ ref('procedure') }} as m
left outer join {{ ref('terminology__hcpcs_level_2') }} as term on m.normalized_code = term.hcpcs
where
    m.normalized_code_type = 'hcpcs_level_2'
)

, others as (
    select
      m.data_source
    , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    , 'PROCEDURE' as table_name
    , 'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') as drill_down_value
    , 'NORMALIZED_CODE' as field_name
    , 'null' as bucket_name
    , 'code type does not have a matching code terminology table' as invalid_reason
    , cast(normalized_code as {{ dbt.type_string() }}) as field_value
from {{ ref('procedure') }} as m
where
    m.normalized_code_type not in ('icd-9-pcs', 'icd-10-pcs','hcpcs_level_2')
)

select *, '{{ var('tuva_last_run') }}' as tuva_last_run from icd9

union all

select * , '{{ var('tuva_last_run') }}' as tuva_last_run from icd10

union all

select * , '{{ var('tuva_last_run') }}' as tuva_last_run from hcpcs_level_2

union all

select * , '{{ var('tuva_last_run') }}' as tuva_last_run from others
