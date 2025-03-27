{{ config(
    enabled = var('claims_enabled', False)
) }}

select distinct -- to bring to claim_ID grain
      m.data_source
    , coalesce(cast(m.paid_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'PHARMACY_CLAIM' as table_name
    , 'Claim ID | Claim Line Number' as drill_down_key
    , {{ concat_custom(["coalesce(cast(m.claim_id as " ~ dbt.type_string() ~ "), 'null')",
                    "'|'",
                    "coalesce(cast(m.claim_line_number as " ~ dbt.type_string() ~ "), 'null')"]) }} as drill_down_value
    , 'PHARMACY' as claim_type
    , 'PRESCRIBING_PROVIDER_NPI' as field_name
    , case when term.npi is not null          then        'valid'
          when m.prescribing_provider_npi is not null    then 'invalid'
                                             else 'null' end as bucket_name
    , case
        when m.prescribing_provider_npi is not null
            and term.npi is null
            then 'Prescribing Provider NPI does not join to Terminology Provider table'
        else null
    end as invalid_reason
    , cast(m.prescribing_provider_npi as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('pharmacy_claim') }} as m
left outer join {{ ref('terminology__provider') }} as term on m.prescribing_provider_npi = term.npi
