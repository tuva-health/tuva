{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT
      m.data_source
    , coalesce(cast(m.claim_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'MEDICAL_CLAIM' AS table_name
    , 'Claim ID | Claim Line Number' AS drill_down_key
    , {{ dbt.concat(["coalesce(m.claim_id, 'null')", "'|'", "coalesce(m.claim_line_number, 'NULL')"]) }} as drill_down_value
    , m.claim_type as claim_type
    , 'HCPCS_CODE' AS field_name
    , case
          when term.hcpcs is not null then 'valid'
          when m.hcpcs_code is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case
        when M.HCPCS_CODE is not null AND TERM.HCPCS is null then 'HCPCS does not join to Terminology HCPCS_LEVEL_2 table'
        else null
     end as invalid_reason
    , {{ dbt.concat(["m.hcpcs_code", "'|'", "coalesce(term.short_description, '')"]) }} as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('medical_claim')}} m
left join {{ ref('terminology__hcpcs_level_2')}} as term on m.hcpcs_code = term.hcpcs
