{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT
      m.data_source
    , coalesce(cast(m.paid_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'PHARMACY_CLAIM' AS table_name
    , 'Claim ID | Claim Line Number' AS drill_down_key
    , {{ dbt.concat(["coalesce(cast(m.claim_id as " ~ dbt.type_string() ~ "), 'null')",
                    "'|'",
                    "coalesce(cast(m.claim_line_number as " ~ dbt.type_string() ~ "), 'NULL')"]) }} as drill_down_value
    , 'PHARMACY' AS claim_type
    , 'PLAN' AS field_name
    {% if target.type == 'fabric' %}
    , case
        when m."plan" is null then 'null' else 'valid' end as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast("plan" as {{ dbt.type_string() }}) as field_value
    {% else %}
    , case
        when m.plan is null then 'null' else 'valid' end as bucket_name
    , cast(null as {{ dbt.type_string() }}) as invalid_reason
    , cast(plan as {{ dbt.type_string() }}) as field_value
    {% endif %}
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('pharmacy_claim')}} m
