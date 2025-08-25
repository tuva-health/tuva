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
    ,'STATE' as field_name
    ,case when m.state is  null then 'null'
          when term.ssa_fips_state_name is null then 'invalid'
                             else 'valid' end as bucket_name
    ,case
        when m.state is not null and term.ssa_fips_state_name is null then 'State does not join to Terminology SSA_FIPS_STATE table'
        else null
    end as invalid_reason
    ,cast(state as {{ dbt.type_string() }}) as field_value
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('eligibility') }} as m
left outer join {{ ref('reference_data__ssa_fips_state') }} as term on m.state = term.ssa_fips_state_name
