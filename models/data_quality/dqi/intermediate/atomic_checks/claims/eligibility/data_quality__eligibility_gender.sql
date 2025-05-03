{{ config(
    enabled = var('claims_enabled', False)
) }}

select distinct 
    m.data_source
    ,coalesce(cast(m.enrollment_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'ELIGIBILITY' as table_name
    ,'Member ID | Enrollment Start Date' as drill_down_key
    ,coalesce(m.member_id, 'NULL') as drill_down_value
    ,'ELIGIBILITY' as claim_type
    ,'GENDER' as field_name
    ,case when m.gender is  null then 'null'
          when term.gender is null then 'invalid'
                             else 'valid' end as bucket_name
    ,case
        when m.gender is not null and term.gender is null then 'Gender does not join to Terminology Gender table'
        else null
    end as invalid_reason
    ,cast(m.gender as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('eligibility') }} as m
left outer join {{ ref('terminology__gender') }} as term on m.gender = term.gender
