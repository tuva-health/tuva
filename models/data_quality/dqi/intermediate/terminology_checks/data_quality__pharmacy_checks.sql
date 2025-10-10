{{ config(
    materialized='ephemeral',
    tags=['dqi','data_quality'],
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/* Centralized pharmacy terminology checks (line-level). */

with p as (
    select * from {{ ref('pharmacy_claim') }}
)

select
    t.data_source,
    t.payer,
    t.plan as {{ quote_column('plan') }},
    t.claim_id,
    t.claim_line_number,
    metric_id,
    'pharmacy' as claim_scope,
    1 as distinct_vals,
    has_valid,
    has_invalid,
    has_null
from (
    -- NDC
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        claim_id,
        claim_line_number,
        'claims:pharmacy:NDC_CODE' as metric_id,
        case when term.ndc is not null then 1 else 0 end as has_valid,
        case when p.ndc_code is not null and term.ndc is null then 1 else 0 end as has_invalid,
        case when p.ndc_code is null then 1 else 0 end as has_null
    from p
    left join {{ ref('terminology__ndc') }} term on p.ndc_code = term.ndc

    union all
    -- Dispensing Provider NPI
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        claim_id,
        claim_line_number,
        'claims:pharmacy:DISPENSING_PROVIDER_NPI' as metric_id,
        case when prov.npi is not null then 1 else 0 end as has_valid,
        case when p.dispensing_provider_npi is not null and prov.npi is null then 1 else 0 end as has_invalid,
        case when p.dispensing_provider_npi is null then 1 else 0 end as has_null
    from p
    left join {{ ref('terminology__provider') }} prov on p.dispensing_provider_npi = prov.npi

    union all
    -- Prescribing Provider NPI
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        claim_id,
        claim_line_number,
        'claims:pharmacy:PRESCRIBING_PROVIDER_NPI' as metric_id,
        case when prov.npi is not null then 1 else 0 end as has_valid,
        case when p.prescribing_provider_npi is not null and prov.npi is null then 1 else 0 end as has_invalid,
        case when p.prescribing_provider_npi is null then 1 else 0 end as has_null
    from p
    left join {{ ref('terminology__provider') }} prov on p.prescribing_provider_npi = prov.npi
) t
