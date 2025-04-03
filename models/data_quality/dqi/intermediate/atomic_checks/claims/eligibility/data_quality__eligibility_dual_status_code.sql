{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT
    m.data_source
    ,coalesce(cast(m.enrollment_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'ELIGIBILITY' AS table_name
    ,'Member ID | Enrollment Start Date' AS drill_down_key
        ,coalesce(m.member_id, 'NULL') as drill_down_value
    ,'ELIGIBILITY' AS claim_type
    ,'DUAL_STATUS_CODE' AS field_name
    ,case when m.dual_status_code is null then 'null'
          when term.dual_status_code is null then 'invalid'
                             else 'valid' end as bucket_name
    ,case
        when m.dual_status_code is not null and term.dual_status_code is null then 'Dual Status Code does not join to Terminology Medicare Dual Eligibility table'
        else null
    end as invalid_reason
    , {{ concat_custom(["m.dual_status_code", "'|'", "coalesce(term.dual_status_description,'')"]) }} as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('eligibility')}} m
left join {{ ref('terminology__medicare_dual_eligibility')}} term on m.dual_status_code = term.dual_status_code
