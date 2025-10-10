{{ config(
    materialized='ephemeral',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/* Centralized medical-claim terminology checks (claim_type). */

with m as (
    select * from {{ ref('medical_claim') }}
)

select
    m.data_source,
    m.payer,
    {{ quote_column('plan') }} as plan,
    m.claim_id,
    m.claim_line_number,
    'claims:medical:CLAIM_TYPE' as metric_id,
    'medical' as claim_scope,
    1 as distinct_vals,
    case when term.claim_type is not null then 1 else 0 end as has_valid,
    case when m.claim_type is not null and term.claim_type is null then 1 else 0 end as has_invalid,
    case when m.claim_type is null then 1 else 0 end as has_null
from m
left join {{ ref('terminology__claim_type') }} term on m.claim_type = term.claim_type

