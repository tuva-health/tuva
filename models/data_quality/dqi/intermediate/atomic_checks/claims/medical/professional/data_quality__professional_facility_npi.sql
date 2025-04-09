{{ config(
    enabled = var('claims_enabled', False)
) }}

with base as (
    select *
    from {{ ref('medical_claim') }}
    where claim_type = 'professional'
)

select
      m.data_source
    , coalesce(cast(m.claim_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'MEDICAL_CLAIM' as table_name
    , 'Claim ID | Claim Line Number' as drill_down_key
    , {{ concat_custom(["coalesce(cast(m.claim_id as " ~ dbt.type_string() ~ "), 'null')",
                    "'|'",
                    "coalesce(cast(m.claim_line_number as " ~ dbt.type_string() ~ "), 'null')"]) }} as drill_down_value
    , 'professional' as claim_type
    , 'FACILITY_NPI' as field_name
    , case when term.npi is not null          then        'valid'
          when m.facility_npi is not null    then 'invalid'
                                             else 'null' end as bucket_name
    , case
        when m.facility_npi is not null
            and term.npi is null
            then 'Facility NPI does not join to Terminology Provider Table'
        else null
    end as invalid_reason
    , cast(m.facility_npi as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from base as m
left outer join {{ ref('terminology__provider') }} as term on m.facility_npi = term.npi
