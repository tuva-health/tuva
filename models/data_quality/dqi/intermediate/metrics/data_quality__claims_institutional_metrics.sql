{{ config(
    materialized='ephemeral',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Aggregated institutional (excluding inpatient-only metrics) terminology metrics
at data_source/payer/plan grain, sourced from centralized checks.
*/

with checks as (
    select * from {{ ref('data_quality__claims_institutional_checks') }}
),

-- Claim-level metrics aggregation
claim_metrics as (
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        metric_id,
        case metric_id
            when 'claims:institutional:BILL_TYPE_CODE' then 'Bill Type Code (Institutional)'
            when 'claims:institutional:BILLING_NPI' then 'Billing NPI (Institutional)'
            when 'claims:institutional:RENDERING_NPI' then 'Rendering NPI (Institutional)'
            when 'claims:institutional:FACILITY_NPI' then 'Facility NPI (Institutional)'
            when 'claims:institutional:DIAGNOSIS_CODE_1' then 'Diagnosis Code 1 (Institutional)'
            when 'claims:institutional:DIAGNOSIS_CODE_2' then 'Diagnosis Code 2 (Institutional)'
            when 'claims:institutional:DIAGNOSIS_CODE_3' then 'Diagnosis Code 3 (Institutional)'
            when 'claims:institutional:ADMIT_SOURCE_CODE' then 'Admit Source Code (Institutional)'
            when 'claims:institutional:ADMIT_TYPE_CODE' then 'Admit Type Code (Institutional)'
            when 'claims:institutional:DISCHARGE_DISPOSITION_CODE' then 'Discharge Disposition (Institutional)'
        end as metric_name,
        'institutional' as claim_scope,
        sum(case when distinct_vals > 1 then 0 when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when distinct_vals > 1 then 0 when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when distinct_vals > 1 then 0 when has_null = 1 then 1 else 0 end) as null_n,
        sum(case when distinct_vals > 1 then 1 else 0 end) as multiple_n,
        count(*) as denominator_n,
        'Institutional claims' as denominator_desc
    from checks
    where claim_scope = 'institutional'
      and metric_id in (
        'claims:institutional:BILL_TYPE_CODE',
        'claims:institutional:BILLING_NPI',
        'claims:institutional:RENDERING_NPI',
        'claims:institutional:FACILITY_NPI',
        'claims:institutional:DIAGNOSIS_CODE_1',
        'claims:institutional:DIAGNOSIS_CODE_2',
        'claims:institutional:DIAGNOSIS_CODE_3',
        'claims:institutional:ADMIT_SOURCE_CODE',
        'claims:institutional:ADMIT_TYPE_CODE',
        'claims:institutional:DISCHARGE_DISPOSITION_CODE'
      )
    group by data_source, payer, {{ quote_column('plan') }}, metric_id
),

-- Line-level metrics aggregation
line_metrics as (
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        metric_id,
        case metric_id
            when 'claims:institutional_outpatient:HCPCS_CODE' then 'HCPCS Code (Institutional Outpatient)'
            when 'claims:institutional:REVENUE_CENTER_CODE' then 'Revenue Center Code (Institutional)'
        end as metric_name,
        case metric_id
            when 'claims:institutional_outpatient:HCPCS_CODE' then 'institutional_outpatient'
            else 'institutional'
        end as claim_scope,
        sum(case when has_valid = 1 then 1 else 0 end) as valid_n,
        sum(case when has_invalid = 1 then 1 else 0 end) as invalid_n,
        sum(case when has_null = 1 then 1 else 0 end) as null_n,
        0 as multiple_n,
        count(*) as denominator_n,
        case metric_id
            when 'claims:institutional_outpatient:HCPCS_CODE' then 'Institutional outpatient claim lines'
            else 'Institutional claim lines'
        end as denominator_desc
    from checks
    where metric_id in (
        'claims:institutional_outpatient:HCPCS_CODE',
        'claims:institutional:REVENUE_CENTER_CODE'
    )
    group by data_source, payer, {{ quote_column('plan') }}, metric_id
)

select * from claim_metrics
union all
select * from line_metrics

