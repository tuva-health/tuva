{{ config(
    enabled = var('claims_enabled', False)
) }}

select distinct
    m.data_source
    ,coalesce(cast(m.enrollment_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'ELIGIBILITY' as table_name
    ,'Member ID' as drill_down_key
    ,coalesce(m.member_id,'NULL') as drill_down_value
    ,'ELIGIBILITY' as claim_type
    ,'ENROLLMENT_END_DATE' as field_name
    ,case
        when m.enrollment_end_date <= cast('1901-01-01' as date) then 'invalid'
        when m.enrollment_end_date < m.enrollment_start_date then 'invalid'
        when m.enrollment_end_date is null then 'null'
        else 'valid'
    end as bucket_name
    ,case

        when m.enrollment_end_date <= cast('1901-01-01' as date) then 'too old'
        when m.enrollment_end_date < m.enrollment_start_date then 'end date before start date'
        else null
    end as invalid_reason
    ,cast(enrollment_end_date as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('eligibility') }} as m
