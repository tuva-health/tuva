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
Centralized professional terminology checks (line-level).
Emits per-claim-line flags for metrics used by professional metrics aggregation.
*/

with mc as (
    select * from {{ ref('medical_claim') }} where claim_type = 'professional'
)

select
    t.data_source,
    t.payer,
    t.plan as {{ quote_column('plan') }},
    t.claim_id,
    t.claim_line_number,
    metric_id,
    'professional' as claim_scope,
    1 as distinct_vals,
    has_valid,
    has_invalid,
    has_null
from (
    -- HCPCS
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        claim_id,
        claim_line_number,
        'claims:professional:HCPCS_CODE' as metric_id,
        case when term.hcpcs is not null then 1 else 0 end as has_valid,
        case when mc.hcpcs_code is not null and term.hcpcs is null then 1 else 0 end as has_invalid,
        case when mc.hcpcs_code is null then 1 else 0 end as has_null
    from mc
    left join {{ ref('terminology__hcpcs_level_2') }} term on mc.hcpcs_code = term.hcpcs

    union all
    -- Place of Service
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        claim_id,
        claim_line_number,
        'claims:professional:PLACE_OF_SERVICE_CODE' as metric_id,
        case when term.place_of_service_code is not null then 1 else 0 end as has_valid,
        case when mc.place_of_service_code is not null and term.place_of_service_code is null then 1 else 0 end as has_invalid,
        case when mc.place_of_service_code is null then 1 else 0 end as has_null
    from mc
    left join {{ ref('terminology__place_of_service') }} term on mc.place_of_service_code = term.place_of_service_code

    union all
    -- Billing NPI
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        claim_id,
        claim_line_number,
        'claims:professional:BILLING_NPI' as metric_id,
        case when prov.npi is not null then 1 else 0 end as has_valid,
        case when mc.billing_npi is not null and prov.npi is null then 1 else 0 end as has_invalid,
        case when mc.billing_npi is null then 1 else 0 end as has_null
    from mc
    left join {{ ref('terminology__provider') }} prov on mc.billing_npi = prov.npi

    union all
    -- Rendering NPI
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        claim_id,
        claim_line_number,
        'claims:professional:RENDERING_NPI' as metric_id,
        case when prov.npi is not null then 1 else 0 end as has_valid,
        case when mc.rendering_npi is not null and prov.npi is null then 1 else 0 end as has_invalid,
        case when mc.rendering_npi is null then 1 else 0 end as has_null
    from mc
    left join {{ ref('terminology__provider') }} prov on mc.rendering_npi = prov.npi

    union all
    -- Facility NPI
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        claim_id,
        claim_line_number,
        'claims:professional:FACILITY_NPI' as metric_id,
        case when prov.npi is not null then 1 else 0 end as has_valid,
        case when mc.facility_npi is not null and prov.npi is null then 1 else 0 end as has_invalid,
        case when mc.facility_npi is null then 1 else 0 end as has_null
    from mc
    left join {{ ref('terminology__provider') }} prov on mc.facility_npi = prov.npi

    union all
    -- Diagnosis Code 1
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        claim_id,
        claim_line_number,
        'claims:professional:DIAGNOSIS_CODE_1' as metric_id,
        case when term.icd_10_cm is not null then 1 else 0 end as has_valid,
        case when mc.diagnosis_code_1 is not null and term.icd_10_cm is null then 1 else 0 end as has_invalid,
        case when mc.diagnosis_code_1 is null then 1 else 0 end as has_null
    from mc
    left join {{ ref('terminology__icd_10_cm') }} term on mc.diagnosis_code_1 = term.icd_10_cm

    union all
    -- Diagnosis Code 2
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        claim_id,
        claim_line_number,
        'claims:professional:DIAGNOSIS_CODE_2' as metric_id,
        case when term.icd_10_cm is not null then 1 else 0 end as has_valid,
        case when mc.diagnosis_code_2 is not null and term.icd_10_cm is null then 1 else 0 end as has_invalid,
        case when mc.diagnosis_code_2 is null then 1 else 0 end as has_null
    from mc
    left join {{ ref('terminology__icd_10_cm') }} term on mc.diagnosis_code_2 = term.icd_10_cm

    union all
    -- Diagnosis Code 3
    select
        data_source,
        payer,
        {{ quote_column('plan') }} as plan,
        claim_id,
        claim_line_number,
        'claims:professional:DIAGNOSIS_CODE_3' as metric_id,
        case when term.icd_10_cm is not null then 1 else 0 end as has_valid,
        case when mc.diagnosis_code_3 is not null and term.icd_10_cm is null then 1 else 0 end as has_invalid,
        case when mc.diagnosis_code_3 is null then 1 else 0 end as has_null
    from mc
    left join {{ ref('terminology__icd_10_cm') }} term on mc.diagnosis_code_3 = term.icd_10_cm
) t
