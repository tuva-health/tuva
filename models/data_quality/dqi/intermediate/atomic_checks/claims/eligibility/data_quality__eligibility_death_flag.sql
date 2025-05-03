{{ config(
    enabled = var('claims_enabled', False)
) }}

select distinct
    m.data_source
    ,coalesce(cast(m.enrollment_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'ELIGIBILITY' as table_name
    ,'Member ID' as drill_down_key
    ,coalesce(m.member_id, 'NULL') as drill_down_value
    ,'ELIGIBILITY' as claim_type
    ,'DEATH_FLAG' as field_name
    ,case
        when cast(cast(m.death_flag as integer) as {{ dbt.type_string() }}) in ('1','0') then 'valid'
        when m.death_flag is null then 'null'
        else 'invalid'
        end as bucket_name
    ,cast(null as {{ dbt.type_string() }}) as invalid_reason
    ,cast(death_flag as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('eligibility') }} as m
