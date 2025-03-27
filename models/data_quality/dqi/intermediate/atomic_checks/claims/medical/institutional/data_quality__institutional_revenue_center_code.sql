{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      m.data_source
    , coalesce(cast(m.claim_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'MEDICAL_CLAIM' as table_name
    , 'Claim ID | Claim Line Number' as drill_down_key
    , {{ concat_custom(["coalesce(cast(m.claim_id as " ~ dbt.type_string() ~ "), 'null')",
                    "'|'",
                    "coalesce(cast(m.claim_line_number as " ~ dbt.type_string() ~ "), 'null')"]) }} as drill_down_value
    , 'institutional' as claim_type
    , 'REVENUE_CENTER_CODE' as field_name
    , case
          when term.revenue_center_code is not null then 'valid'
          when m.revenue_center_code is not null    then 'invalid'
                                                    else 'null' end as bucket_name
    , case
        when m.revenue_center_code is not null
            and term.revenue_center_code is null
            then 'Revenue center code does not join to Terminology Revenue Center table'
        else null
    end as invalid_reason
    , {{ concat_custom(["m.revenue_center_code", "'|'", "coalesce(term.revenue_center_description, '')"]) }} as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
    from {{ ref('medical_claim') }} as m
left outer join {{ ref('terminology__revenue_center') }} as term on m.revenue_center_code = term.revenue_center_code
