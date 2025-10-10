{{ config(
    materialized='ephemeral',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Aggregated institutional inpatient metrics (DRG, ICD-10-PCS procedures 1-3)
at data_source/payer/plan grain.

Refactored to source per-claim flags from a single centralized checks model
to avoid repeated scans of medical_claim and duplicate case logic.
*/

with per_claim as (
    select *
    from {{ ref('data_quality__claims_institutional_inpatient_checks') }}
),

meta as (
    select 'claims:institutional_inpatient:DRG_CODE' as metric_id, 'DRG Code (Inpatient)' as metric_name
    union all select 'claims:institutional_inpatient:PROCEDURE_CODE_1', 'Procedure Code 1 (Inpatient)'
    union all select 'claims:institutional_inpatient:PROCEDURE_CODE_2', 'Procedure Code 2 (Inpatient)'
    union all select 'claims:institutional_inpatient:PROCEDURE_CODE_3', 'Procedure Code 3 (Inpatient)'
)

select
    pc.data_source,
    pc.payer,
    {{ quote_column('plan') }} as plan,
    pc.metric_id,
    m.metric_name,
    'institutional_inpatient' as claim_scope,
    sum(case when pc.distinct_vals > 1 then 0 when pc.has_valid = 1 then 1 else 0 end) as valid_n,
    sum(case when pc.distinct_vals > 1 then 0 when pc.has_invalid = 1 then 1 else 0 end) as invalid_n,
    sum(case when pc.distinct_vals > 1 then 0 when pc.has_null = 1 then 1 else 0 end) as null_n,
    sum(case when pc.distinct_vals > 1 then 1 else 0 end) as multiple_n,
    count(*) as denominator_n,
    'Institutional inpatient claims' as denominator_desc
from per_claim pc
join meta m on pc.metric_id = m.metric_id
group by pc.data_source, pc.payer, {{ quote_column('plan') }}, pc.metric_id, m.metric_name
