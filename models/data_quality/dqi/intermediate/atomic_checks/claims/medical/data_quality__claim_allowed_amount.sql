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
    , 'ALLOWED_AMOUNT' AS field_name
    , case when m.allowed_amount is null then 'null'
                                    else 'valid' end as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast(allowed_amount as {{ dbt.type_string() }}) as field_value
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medical_claim')}} m
