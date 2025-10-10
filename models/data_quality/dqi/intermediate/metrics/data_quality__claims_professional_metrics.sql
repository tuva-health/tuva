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
Aggregated professional-claims terminology metrics at data_source/payer/plan grain.
Outputs columns matching data_quality__terminology_metrics union schema:
 - data_source, payer, plan, metric_id, metric_name, claim_scope,
   valid_n, invalid_n, null_n, multiple_n, denominator_n, denominator_desc
*/

with checks as (
    select * from {{ ref('data_quality__claims_professional_checks') }}
)

select
    data_source,
    payer,
    {{ quote_column('plan') }} as plan,
    metric_id,
    case metric_id
        when 'claims:professional:HCPCS_CODE' then 'HCPCS Code (Professional)'
        when 'claims:professional:PLACE_OF_SERVICE_CODE' then 'Place of Service (Professional)'
        when 'claims:professional:BILLING_NPI' then 'Billing NPI (Professional)'
        when 'claims:professional:RENDERING_NPI' then 'Rendering NPI (Professional)'
        when 'claims:professional:FACILITY_NPI' then 'Facility NPI (Professional)'
        when 'claims:professional:DIAGNOSIS_CODE_1' then 'Diagnosis Code 1 (Professional)'
        when 'claims:professional:DIAGNOSIS_CODE_2' then 'Diagnosis Code 2 (Professional)'
        when 'claims:professional:DIAGNOSIS_CODE_3' then 'Diagnosis Code 3 (Professional)'
    end as metric_name,
    'professional' as claim_scope,
    sum(case when has_valid = 1 then 1 else 0 end) as valid_n,
    sum(case when has_invalid = 1 then 1 else 0 end) as invalid_n,
    sum(case when has_null = 1 then 1 else 0 end) as null_n,
    0 as multiple_n,
    count(*) as denominator_n,
    'Professional claim lines' as denominator_desc
from checks
group by data_source, payer, {{ quote_column('plan') }}, metric_id
