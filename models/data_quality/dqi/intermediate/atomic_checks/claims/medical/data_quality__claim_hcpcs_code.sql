{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT
      m.data_source
    , coalesce(cast(m.claim_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'MEDICAL_CLAIM' AS table_name
    , 'Claim ID | Claim Line Number' AS drill_down_key
    , {{ concat_custom(["coalesce(cast(m.claim_id as " ~ dbt.type_string() ~ "), 'null')",
                    "'|'",
                    "coalesce(cast(m.claim_line_number as " ~ dbt.type_string() ~ "), 'null')"]) }} as drill_down_value
    , m.claim_type as claim_type
    , 'HCPCS_CODE' AS field_name
    , case
          when term.hcpcs is not null then 'valid'
          when m.hcpcs_code is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case
        when m.hcpcs_code is not null and term.hcpcs is null then 'HCPCS does not join to Terminology HCPCS_LEVEL_2 table'
        else null
     end as invalid_reason
    , {{ concat_custom(["m.hcpcs_code", "'|'", "coalesce(term.short_description, '')"]) }} as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim')}} m
left join {{ ref('terminology__hcpcs_level_2')}} as term on m.hcpcs_code = term.hcpcs
