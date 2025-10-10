{{ config(
    materialized='table',
    enabled = (
        var('enable_input_layer_testing', true) | as_bool
    ) and (
        var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool
    )
) }}

/*
Terminology metrics (non-PHI) aggregated at uniform grain data_source/payer/plan.
This model unions modular atomic metrics models for clarity and maintainability.

Columns:
 - data_source, payer, plan, metric_id, metric_name, claim_scope,
   denominator_n, valid_n, invalid_n, null_n, multiple_n, valid_pct,
   threshold, pass_flag, denominator_desc, tuva_last_run
*/

with unioned as (
    select * from {{ ref('data_quality__claims_institutional_inpatient_metrics') }}
    union all
    select * from {{ ref('data_quality__claims_institutional_metrics') }}
    union all
    select * from {{ ref('data_quality__claims_professional_metrics') }}
    union all
    select * from {{ ref('data_quality__pharmacy_metrics') }}
    union all
    select * from {{ ref('data_quality__eligibility_metrics') }}
    union all
    select * from {{ ref('data_quality__medical_claim_type_metric') }}
)

select
    data_source,
    payer,
    {{ quote_column('plan') }} as plan,
    metric_id,
    metric_name,
    claim_scope,
    denominator_n,
    valid_n,
    invalid_n,
    null_n,
    multiple_n,
    case when denominator_n > 0 then 1.0 * valid_n / denominator_n else null end as valid_pct,
    case when metric_id = 'claims:institutional_outpatient:HCPCS_CODE' then 0.80 else 0.97 end as threshold,
    case when denominator_n > 0 and (1.0 * valid_n / denominator_n) >= case when metric_id = 'claims:institutional_outpatient:HCPCS_CODE' then 0.80 else 0.97 end and multiple_n = 0 then true else false end as pass_flag,
    denominator_desc,
    '{{ var('tuva_last_run') }}' as tuva_last_run
from unioned

