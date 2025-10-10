{{ config(
    materialized='ephemeral',
    tags=['dqi','data_quality'],
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Aggregated eligibility terminology metrics at data_source/payer/plan grain.
*/

with checks as (
    select * from {{ ref('data_quality__eligibility_checks') }}
)

select
    data_source,
    payer,
    {{ quote_column('plan') }} as plan,
    metric_id,
    case metric_id
        when 'claims:eligibility:GENDER' then 'Gender (Eligibility)'
        when 'claims:eligibility:RACE' then 'Race (Eligibility)'
        when 'claims:eligibility:PAYER_TYPE' then 'Payer Type (Eligibility)'
        when 'claims:eligibility:MEDICARE_STATUS_CODE' then 'Medicare Status Code (Eligibility)'
        when 'claims:eligibility:DUAL_STATUS_CODE' then 'Dual Status Code (Eligibility)'
        when 'claims:eligibility:ORIGINAL_REASON_ENTITLEMENT_CODE' then 'Original Reason Entitlement Code (Eligibility)'
    end as metric_name,
    'eligibility' as claim_scope,
    sum(case when has_valid = 1 then 1 else 0 end) as valid_n,
    sum(case when has_invalid = 1 then 1 else 0 end) as invalid_n,
    sum(case when has_null = 1 then 1 else 0 end) as null_n,
    0 as multiple_n,
    count(*) as denominator_n,
    'Eligibility records' as denominator_desc
from checks
group by data_source, payer, {{ quote_column('plan') }}, metric_id
