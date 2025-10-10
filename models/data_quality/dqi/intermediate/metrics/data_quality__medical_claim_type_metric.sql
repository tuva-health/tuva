{{ config(
    materialized='ephemeral',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Aggregated medical claim_type terminology metric at data_source/payer/plan grain.
*/

with checks as (
    select * from {{ ref('data_quality__medical_claim_checks') }}
)

select
    data_source,
    payer,
    {{ quote_column('plan') }} as plan,
    'claims:medical:CLAIM_TYPE' as metric_id,
    'Claim Type (Medical)'      as metric_name,
    'medical'                   as claim_scope,
    sum(case when has_valid = 1 then 1 else 0 end) as valid_n,
    sum(case when has_invalid = 1 then 1 else 0 end) as invalid_n,
    sum(case when has_null = 1 then 1 else 0 end) as null_n,
    0 as multiple_n,
    count(*) as denominator_n,
    'All medical claims' as denominator_desc
from checks
group by data_source, payer, {{ quote_column('plan') }}
