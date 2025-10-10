{{ config(
    materialized='ephemeral',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Aggregated pharmacy terminology metrics at data_source/payer/plan grain.
*/

with checks as (
    select * from {{ ref('data_quality__pharmacy_checks') }}
)

select
    data_source,
    payer,
    {{ quote_column('plan') }} as plan,
    metric_id,
    case metric_id
        when 'claims:pharmacy:NDC_CODE' then 'NDC Code (Pharmacy)'
        when 'claims:pharmacy:DISPENSING_PROVIDER_NPI' then 'Dispensing Provider NPI (Pharmacy)'
        when 'claims:pharmacy:PRESCRIBING_PROVIDER_NPI' then 'Prescribing Provider NPI (Pharmacy)'
    end as metric_name,
    'pharmacy' as claim_scope,
    sum(case when has_valid = 1 then 1 else 0 end) as valid_n,
    sum(case when has_invalid = 1 then 1 else 0 end) as invalid_n,
    sum(case when has_null = 1 then 1 else 0 end) as null_n,
    0 as multiple_n,
    count(*) as denominator_n,
    'Pharmacy claim lines' as denominator_desc
from checks
group by data_source, payer, {{ quote_column('plan') }}, metric_id
