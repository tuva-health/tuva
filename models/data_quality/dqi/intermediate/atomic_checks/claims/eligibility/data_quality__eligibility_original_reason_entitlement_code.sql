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
    ,'ORIGINAL_REASON_ENTITLEMENT_CODE' as field_name
    ,case when m.original_reason_entitlement_code is null then 'null'
          when term.original_reason_entitlement_code is null then 'invalid'
                             else 'valid' end as bucket_name
    ,case
        when m.original_reason_entitlement_code is not null and term.original_reason_entitlement_code is null then 'Original Reason Entitlement Code does not join to Terminology Original Reason Entitlement Code table'
        else null
    end as invalid_reason
    , {{ concat_custom(["m.original_reason_entitlement_code", "'|'", "coalesce(term.original_reason_entitlement_description,'')"]) }} as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('eligibility') }} as m
left outer join {{ ref('terminology__medicare_orec') }} as term on m.original_reason_entitlement_code = term.original_reason_entitlement_code
