{{ config(
    enabled = var('claims_enabled', False)
) }}
with tuva_last_run as(
     select cast(substring('{{ var('tuva_last_run') }}',1,10) as date) as tuva_last_run
)

SELECT
      m.data_source
    , coalesce(cast(m.paid_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'PHARMACY_CLAIM' AS table_name
    , 'Claim ID | Claim Line Number' AS drill_down_key
    , {{ concat_custom(["coalesce(cast(m.claim_id as " ~ dbt.type_string() ~ "), 'null')",
                    "'|'",
                    "coalesce(cast(m.claim_line_number as " ~ dbt.type_string() ~ "), 'null')"]) }} as drill_down_value
    , 'PHARMACY' AS claim_type
    , 'PAID_DATE' AS field_name
    , case
        when m.paid_date > tuva_last_run then 'invalid'
        when m.paid_date < {{ dbt.dateadd(datepart="year", interval=-10, from_date_or_timestamp="cte.tuva_last_run") }} then 'invalid'
        when m.paid_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.paid_date > tuva_last_run then 'future'
        when m.paid_date < {{ dbt.dateadd(datepart="year", interval=-10, from_date_or_timestamp="cte.tuva_last_run") }} then 'too old'
        else null
        end as invalid_reason
    , cast(paid_date as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('pharmacy_claim')}} m
cross join tuva_last_run cte
