{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT
    m.data_source
    ,coalesce(cast(m.enrollment_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'ELIGIBILITY' AS table_name
    ,'Member ID' AS drill_down_key
    ,coalesce(m.member_id, 'NULL') as drill_down_value
    ,'ELIGIBILITY' AS claim_type
    ,'DEATH_FLAG' AS field_name
    ,case
        when m.death_flag in (1,0) then 'valid'
        when m.death_flag is null then 'null'
        else 'invalid'
        end as bucket_name
    ,cast(null as {{ dbt.type_string() }}) as invalid_reason
    ,cast(death_flag as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('eligibility')}} m