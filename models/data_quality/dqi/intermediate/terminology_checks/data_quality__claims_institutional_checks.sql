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
Centralized institutional (non-inpatient-specific) terminology checks.
Emits rows for both claim-level and line-level metrics.

Columns emitted:
- data_source, payer, plan, claim_id, claim_line_number (nullable),
  metric_id, claim_scope, distinct_vals, has_valid, has_invalid, has_null
*/

with mc as (
    select * from {{ ref('medical_claim') }} where claim_type = 'institutional'
),

-- Claim-level checks (use distinct across values per claim_id)
bill_type_per_claim as (
    select
        b.data_source,
        b.payer,
        {{ quote_column('plan') }} as plan,
        b.claim_id,
        cast(null as {{ dbt.type_int() }}) as claim_line_number,
        'claims:institutional:BILL_TYPE_CODE' as metric_id,
        'institutional' as claim_scope,
        count(distinct case when b.bill_type_code is not null then b.bill_type_code end) as distinct_vals,
        max(case when b.bill_type_code is not null and term.bill_type_code is not null then 1 else 0 end) as has_valid,
        max(case when b.bill_type_code is not null and term.bill_type_code is null then 1 else 0 end) as has_invalid,
        max(case when b.bill_type_code is null then 1 else 0 end) as has_null
    from mc b
    left join {{ ref('terminology__bill_type') }} term on b.bill_type_code = term.bill_type_code
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
),

inst_billing_npi_per_claim as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
        cast(null as {{ dbt.type_int() }}) as claim_line_number,
        'claims:institutional:BILLING_NPI' as metric_id,
        'institutional' as claim_scope,
        count(distinct case when b.billing_npi is not null then b.billing_npi end) as distinct_vals,
        max(case when b.billing_npi is not null and prov.npi is not null then 1 else 0 end) as has_valid,
        max(case when b.billing_npi is not null and prov.npi is null then 1 else 0 end) as has_invalid,
        max(case when b.billing_npi is null then 1 else 0 end) as has_null
    from mc b
    left join {{ ref('terminology__provider') }} prov on b.billing_npi = prov.npi
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
),

inst_rendering_npi_per_claim as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
        cast(null as {{ dbt.type_int() }}) as claim_line_number,
        'claims:institutional:RENDERING_NPI' as metric_id,
        'institutional' as claim_scope,
        count(distinct case when b.rendering_npi is not null then b.rendering_npi end) as distinct_vals,
        max(case when b.rendering_npi is not null and prov.npi is not null then 1 else 0 end) as has_valid,
        max(case when b.rendering_npi is not null and prov.npi is null then 1 else 0 end) as has_invalid,
        max(case when b.rendering_npi is null then 1 else 0 end) as has_null
    from mc b
    left join {{ ref('terminology__provider') }} prov on b.rendering_npi = prov.npi
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
),

inst_facility_npi_per_claim as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
        cast(null as {{ dbt.type_int() }}) as claim_line_number,
        'claims:institutional:FACILITY_NPI' as metric_id,
        'institutional' as claim_scope,
        count(distinct case when b.facility_npi is not null then b.facility_npi end) as distinct_vals,
        max(case when b.facility_npi is not null and prov.npi is not null then 1 else 0 end) as has_valid,
        max(case when b.facility_npi is not null and prov.npi is null then 1 else 0 end) as has_invalid,
        max(case when b.facility_npi is null then 1 else 0 end) as has_null
    from mc b
    left join {{ ref('terminology__provider') }} prov on b.facility_npi = prov.npi
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
),

inst_dx1_per_claim as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
        cast(null as {{ dbt.type_int() }}) as claim_line_number,
        'claims:institutional:DIAGNOSIS_CODE_1' as metric_id,
        'institutional' as claim_scope,
        count(distinct case when b.diagnosis_code_1 is not null then b.diagnosis_code_1 end) as distinct_vals,
        max(case when b.diagnosis_code_1 is not null and term.icd_10_cm is not null then 1 else 0 end) as has_valid,
        max(case when b.diagnosis_code_1 is not null and term.icd_10_cm is null then 1 else 0 end) as has_invalid,
        max(case when b.diagnosis_code_1 is null then 1 else 0 end) as has_null
    from mc b
    left join {{ ref('terminology__icd_10_cm') }} term on b.diagnosis_code_1 = term.icd_10_cm
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
),

inst_dx2_per_claim as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
        cast(null as {{ dbt.type_int() }}) as claim_line_number,
        'claims:institutional:DIAGNOSIS_CODE_2' as metric_id,
        'institutional' as claim_scope,
        count(distinct case when b.diagnosis_code_2 is not null then b.diagnosis_code_2 end) as distinct_vals,
        max(case when b.diagnosis_code_2 is not null and term.icd_10_cm is not null then 1 else 0 end) as has_valid,
        max(case when b.diagnosis_code_2 is not null and term.icd_10_cm is null then 1 else 0 end) as has_invalid,
        max(case when b.diagnosis_code_2 is null then 1 else 0 end) as has_null
    from mc b
    left join {{ ref('terminology__icd_10_cm') }} term on b.diagnosis_code_2 = term.icd_10_cm
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
),

inst_dx3_per_claim as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
        cast(null as {{ dbt.type_int() }}) as claim_line_number,
        'claims:institutional:DIAGNOSIS_CODE_3' as metric_id,
        'institutional' as claim_scope,
        count(distinct case when b.diagnosis_code_3 is not null then b.diagnosis_code_3 end) as distinct_vals,
        max(case when b.diagnosis_code_3 is not null and term.icd_10_cm is not null then 1 else 0 end) as has_valid,
        max(case when b.diagnosis_code_3 is not null and term.icd_10_cm is null then 1 else 0 end) as has_invalid,
        max(case when b.diagnosis_code_3 is null then 1 else 0 end) as has_null
    from mc b
    left join {{ ref('terminology__icd_10_cm') }} term on b.diagnosis_code_3 = term.icd_10_cm
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
),

inst_admit_source_per_claim as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
        cast(null as {{ dbt.type_int() }}) as claim_line_number,
        'claims:institutional:ADMIT_SOURCE_CODE' as metric_id,
        'institutional' as claim_scope,
        count(distinct case when b.admit_source_code is not null then b.admit_source_code end) as distinct_vals,
        max(case when b.admit_source_code is not null and term.admit_source_code is not null then 1 else 0 end) as has_valid,
        max(case when b.admit_source_code is not null and term.admit_source_code is null then 1 else 0 end) as has_invalid,
        max(case when b.admit_source_code is null then 1 else 0 end) as has_null
    from mc b
    left join {{ ref('terminology__admit_source') }} term on b.admit_source_code = term.admit_source_code
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
),

inst_admit_type_per_claim as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
        cast(null as {{ dbt.type_int() }}) as claim_line_number,
        'claims:institutional:ADMIT_TYPE_CODE' as metric_id,
        'institutional' as claim_scope,
        count(distinct case when b.admit_type_code is not null then b.admit_type_code end) as distinct_vals,
        max(case when b.admit_type_code is not null and term.admit_type_code is not null then 1 else 0 end) as has_valid,
        max(case when b.admit_type_code is not null and term.admit_type_code is null then 1 else 0 end) as has_invalid,
        max(case when b.admit_type_code is null then 1 else 0 end) as has_null
    from mc b
    left join {{ ref('terminology__admit_type') }} term on b.admit_type_code = term.admit_type_code
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
),

inst_discharge_disposition_per_claim as (
    select
        b.data_source, b.payer, {{ quote_column('plan') }} as plan, b.claim_id,
        cast(null as {{ dbt.type_int() }}) as claim_line_number,
        'claims:institutional:DISCHARGE_DISPOSITION_CODE' as metric_id,
        'institutional' as claim_scope,
        count(distinct case when b.discharge_disposition_code is not null then b.discharge_disposition_code end) as distinct_vals,
        max(case when b.discharge_disposition_code is not null and term.discharge_disposition_code is not null then 1 else 0 end) as has_valid,
        max(case when b.discharge_disposition_code is not null and term.discharge_disposition_code is null then 1 else 0 end) as has_invalid,
        max(case when b.discharge_disposition_code is null then 1 else 0 end) as has_null
    from mc b
    left join {{ ref('terminology__discharge_disposition') }} term on b.discharge_disposition_code = term.discharge_disposition_code
    group by b.data_source, b.payer, {{ quote_column('plan') }}, b.claim_id
),

-- Line-level checks
revenue_center_per_line as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        mc.claim_id,
        mc.claim_line_number,
        'claims:institutional:REVENUE_CENTER_CODE' as metric_id,
        'institutional' as claim_scope,
        1 as distinct_vals,
        case when term.revenue_center_code is not null then 1 else 0 end as has_valid,
        case when mc.revenue_center_code is not null and term.revenue_center_code is null then 1 else 0 end as has_invalid,
        case when mc.revenue_center_code is null then 1 else 0 end as has_null
    from mc
    left join {{ ref('terminology__revenue_center') }} term on mc.revenue_center_code = term.revenue_center_code
),

hcpcs_outpatient_per_line as (
    select
        mc.data_source,
        mc.payer,
        {{ quote_column('plan') }} as plan,
        mc.claim_id,
        mc.claim_line_number,
        'claims:institutional_outpatient:HCPCS_CODE' as metric_id,
        'institutional_outpatient' as claim_scope,
        1 as distinct_vals,
        case when term.hcpcs is not null then 1 else 0 end as has_valid,
        case when mc.hcpcs_code is not null and term.hcpcs is null then 1 else 0 end as has_invalid,
        case when mc.hcpcs_code is null then 1 else 0 end as has_null
    from mc
    left join {{ ref('terminology__hcpcs_level_2') }} term on mc.hcpcs_code = term.hcpcs
    where {{ substring('mc.bill_type_code', 1, 2) }} != '11'
)

select * from bill_type_per_claim
union all select * from inst_billing_npi_per_claim
union all select * from inst_rendering_npi_per_claim
union all select * from inst_facility_npi_per_claim
union all select * from inst_dx1_per_claim
union all select * from inst_dx2_per_claim
union all select * from inst_dx3_per_claim
union all select * from inst_admit_source_per_claim
union all select * from inst_admit_type_per_claim
union all select * from inst_discharge_disposition_per_claim
union all select * from revenue_center_per_line
union all select * from hcpcs_outpatient_per_line
