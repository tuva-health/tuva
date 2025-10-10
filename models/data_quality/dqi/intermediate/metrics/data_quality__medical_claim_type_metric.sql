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

select
    m.data_source,
    m.payer,
    {{ quote_column('plan') }} as plan,
    'claims:medical:CLAIM_TYPE' as metric_id,
    'Claim Type (Medical)'      as metric_name,
    'medical'                   as claim_scope,
    sum(case when term.claim_type is not null then 1 else 0 end) as valid_n,
    sum(case when m.claim_type is not null and term.claim_type is null then 1 else 0 end) as invalid_n,
    sum(case when m.claim_type is null then 1 else 0 end) as null_n,
    0 as multiple_n,
    count(*) as denominator_n,
    'All medical claims' as denominator_desc
from {{ ref('medical_claim') }} m
left join {{ ref('terminology__claim_type') }} term on m.claim_type = term.claim_type
group by m.data_source, m.payer, {{ quote_column('plan') }}

